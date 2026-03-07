"""
=============================================================
 SILVER LAYER – REQUEST CLEAN
 Bronze: s3a://bronze/sorted_request_table/
 Silver: s3a://silver/request_clean/
=============================================================
 Performs streaming deduplication and cleaning for
 taxi request events only (NO JOIN).
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

REQUEST_BRONZE_PATH = "s3a://bronze/sorted_request_table"

SILVER_PATH    = "s3a://silver/request_clean"
CHECKPOINT_REQ = "s3a://silver/checkpoints/request_clean"

WATERMARK_DELAY = "10 minutes"

# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RequestClean-Bronze-To-Silver")

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
# READ REQUEST STREAM
# ─────────────────────────────────────────────────────────────
request_bronze = (
    spark.readStream
    .format("delta")
    .load(REQUEST_BRONZE_PATH)
)

# ─────────────────────────────────────────────────────────────
# CLEAN REQUEST DATA
# ─────────────────────────────────────────────────────────────
request_clean = (
    request_bronze

    # filter null
    .filter(col("trip_id").isNotNull())
    .filter(col("event_ts").isNotNull())

    # watermark
    # .withWatermark("event_ts", WATERMARK_DELAY)

    # deduplicate
    .dropDuplicates(["trip_id", "event_ts"])

    # location filter
    .filter((col("pu_location_id") > 0) & (col("pu_location_id") <= 265))

    # select columns
    .select(
        col("trip_id"),
        col("event_ts"),

        col("hvfhs_license_num"),
        col("dispatching_base_num"),

        col("pu_location_id"),
        col("do_location_id"),

        col("trip_miles"),
        col("trip_time"),

        col("base_passenger_fare"),
        col("tips"),
        col("tolls"),
        col("total_amount")
    )

    # metadata
    .withColumn("silver_ingest_time", current_timestamp())

    # partition columns
    .withColumn("year", year(col("event_ts")))
    .withColumn("month", month(col("event_ts")))
    .withColumn("day", dayofmonth(col("event_ts")))
)

# ─────────────────────────────────────────────────────────────
# WRITE TO SILVER
# ─────────────────────────────────────────────────────────────
query = (
    request_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_REQ)
    .partitionBy("year", "month", "day")
    .trigger(processingTime="1 seconds")
    .start(SILVER_PATH)
)

print("[INFO] ✅ Request Clean streaming started")
print(f"[INFO] Silver path: {SILVER_PATH}")
print(f"[INFO] Checkpoint: {CHECKPOINT_REQ}")

query.awaitTermination()