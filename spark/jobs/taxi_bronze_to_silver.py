"""
=============================================================
 SILVER LAYER – TAXI CLEAN (JOIN REQUEST + RESPONSE)
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp,
    year, month, dayofmonth
)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"

REQUEST_BRONZE_PATH  = "s3a://bronze/sorted_request_table"
RESPONSE_BRONZE_PATH = "s3a://bronze/sorted_response_table"

SILVER_PATH     = "s3a://silver/taxi_clean"
CHECKPOINT_PATH = "s3a://silver/checkpoints/taxi_clean_join"

WATERMARK_DELAY = "10 minutes"

# ─────────────────────────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TaxiClean-Bronze-To-Silver")

    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# 1. READ REQUEST STREAM
# ─────────────────────────────────────────────────────────────
request_stream = (
    spark.readStream
    .format("delta")
    .load(REQUEST_BRONZE_PATH)

    .filter(col("trip_id").isNotNull())
    .filter(col("event_ts").isNotNull())

    # .withWatermark("event_ts", WATERMARK_DELAY)
)

# ─────────────────────────────────────────────────────────────
# 2. READ RESPONSE STREAM
# ─────────────────────────────────────────────────────────────
response_stream = (
    spark.readStream
    .format("delta")
    .load(RESPONSE_BRONZE_PATH)

    .filter(col("trip_id").isNotNull())
    .filter(col("event_ts").isNotNull())

    # .withWatermark("event_ts", WATERMARK_DELAY)
)

# ─────────────────────────────────────────────────────────────
# 3. STREAM JOIN
# ─────────────────────────────────────────────────────────────
joined_stream = (
    request_stream.alias("req")
    .join(
        response_stream.alias("res"),
        col("req.trip_id") == col("res.trip_id"),
        "inner"
    )
)

# ─────────────────────────────────────────────────────────────
# 4. CLEAN DATA
# ─────────────────────────────────────────────────────────────
silver_df = (
    joined_stream
    .select(
        col("req.trip_id"),

        col("req.event_ts").alias("request_ts"),
        col("res.event_ts").alias("response_ts"),

        col("req.hvfhs_license_num"),
        col("req.dispatching_base_num"),

        col("req.pu_location_id"),
        col("req.do_location_id"),

        col("req.trip_miles"),
        col("req.trip_time"),

        col("req.base_passenger_fare"),
        col("req.tips"),
        col("req.tolls"),
        col("req.total_amount")
    )

    .filter((col("trip_miles") >= 0) | col("trip_miles").isNull())
    .filter((col("trip_time") >= 0) | col("trip_time").isNull())

    .withColumn("silver_ingest_time", current_timestamp())

    .withColumn("year", year(col("request_ts")))
    .withColumn("month", month(col("request_ts")))
    .withColumn("day", dayofmonth(col("request_ts")))
)

# ─────────────────────────────────────────────────────────────
# 5. WRITE STREAM
# ─────────────────────────────────────────────────────────────
query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("year", "month", "day")
    .trigger(processingTime="1 seconds")
    .start(SILVER_PATH)
)

print("✓ Silver streaming started")

query.awaitTermination()