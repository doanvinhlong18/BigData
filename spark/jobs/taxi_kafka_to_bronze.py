"""
spark/jobs/taxi_kafka_to_bronze.py
──────────────────────────────────
Kafka → Bronze (3 bảng Delta):
  bronze/sorted_request_table   ← event_type == "request"
  bronze/sorted_pickup_table    ← event_type == "pickup"   (NEW)
  bronze/sorted_response_table  ← event_type == "dropoff"

Trigger mỗi 2s, partition by year/month/day theo event_time.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    year,
    month,
    dayofmonth,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "nyc_taxi_events"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
CHECKPOINT_BASE = "s3a://checkpoints/bronze"
BRONZE_REQUEST = "s3a://bronze/request"
BRONZE_PICKUP = "s3a://bronze/pickup"
BRONZE_DROPOFF = "s3a://bronze/dropoff"

# ── Superset schema — union của 3 event types ─────────────────────────────────
PARSE_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        # request fields
        StructField("trip_id", StringType()),
        StructField("hvfhs_license_num", StringType()),
        StructField("dispatching_base_num", StringType()),
        StructField("originating_base_num", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("request_datetime", StringType()),
        StructField("wav_request_flag", StringType()),
        StructField("access_a_ride_flag", StringType()),
        StructField("shared_request_flag", StringType()),
        # pickup fields
        StructField("pickup_datetime", StringType()),
        StructField("on_scene_datetime", StringType()),
        # dropoff fields
        StructField("dropoff_datetime", StringType()),
        StructField("trip_miles", DoubleType()),
        StructField("trip_time", IntegerType()),
        StructField("base_passenger_fare", DoubleType()),
        StructField("driver_pay", DoubleType()),
        StructField("tips", DoubleType()),
        StructField("tolls", DoubleType()),
        StructField("bcf", DoubleType()),
        StructField("sales_tax", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("shared_match_flag", StringType()),
        StructField("wav_match_flag", StringType()),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("taxi_kafka_to_bronze")
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

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    )

    parsed = raw.select(
        from_json(col("json_str"), PARSE_SCHEMA).alias("d"),
        col("kafka_ts"),
    ).select("d.*", "kafka_ts")

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # ── EVENT_TIME → partition columns ───────────────────────────────────
        batch_ts = (
            batch_df.withColumn("_ts", to_timestamp(col("event_time")))
            .withColumn("year", year(col("_ts")))
            .withColumn("month", month(col("_ts")))
            .withColumn("day", dayofmonth(col("_ts")))
        )

        # ── REQUEST ──────────────────────────────────────────────────────────
        req = batch_ts.filter(col("event_type") == "request").select(
            "trip_id",
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "PULocationID",
            "DOLocationID",
            to_timestamp(col("request_datetime")).alias("request_datetime"),
            "wav_request_flag",
            "access_a_ride_flag",
            "shared_request_flag",
            "year",
            "month",
            "day",
        )
        if not req.isEmpty():
            (
                req.write.format("delta")
                .mode("append")
                .partitionBy("year", "month", "day")
                .option("mergeSchema", "true")
                .save(BRONZE_REQUEST)
            )

        # ── PICKUP ───────────────────────────────────────────────────────────
        # Đổi shared_match_flag → share_match_flag để đồng bộ tên với notebook
        # và silver_to_gold (silver/response dùng share_match_flag)
        pu = batch_ts.filter(col("event_type") == "pickup").select(
            "trip_id",
            to_timestamp(col("pickup_datetime")).alias("pickup_datetime"),
            to_timestamp(col("on_scene_datetime")).alias("on_scene_datetime"),
            "PULocationID",
            "year",
            "month",
            "day",
            col("shared_match_flag").alias("share_match_flag"),
            "wav_match_flag",
        )
        if not pu.isEmpty():
            (
                pu.write.format("delta")
                .mode("append")
                .partitionBy("year", "month", "day")
                .option("mergeSchema", "true")
                .save(BRONZE_PICKUP)
            )

        # ── DROPOFF ──────────────────────────────────────────────────────────
        do = batch_ts.filter(col("event_type") == "dropoff").select(
            "trip_id",
            to_timestamp(col("dropoff_datetime")).alias("dropoff_datetime"),
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
            "year",
            "month",
            "day",
        )
        if not do.isEmpty():
            (
                do.write.format("delta")
                .mode("append")
                .partitionBy("year", "month", "day")
                .option("mergeSchema", "true")
                .save(BRONZE_DROPOFF)
            )

    query = (
        parsed.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/kafka_to_bronze")
        .trigger(processingTime="2 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
