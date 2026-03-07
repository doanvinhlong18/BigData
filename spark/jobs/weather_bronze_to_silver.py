"""
=============================================================
 SILVER LAYER – WEATHER CLEAN
 Bronze: s3a://bronze/weather/
 Silver: s3a://silver/weather_clean/
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp,
    year, month, dayofmonth, hour
)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"

BRONZE_PATH       = "s3a://bronze/weather"
SILVER_PATH       = "s3a://silver/weather_clean"
CHECKPOINT_PATH   = "s3a://silver/checkpoints/weather_clean"

WATERMARK_DELAY   = "10 minutes"

# ─────────────────────────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("WeatherClean-Bronze-To-Silver")

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
# READ BRONZE STREAM
# ─────────────────────────────────────────────────────────────
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)

    .filter(col("location_id").isNotNull())
    .filter(col("event_time").isNotNull())

    # .withWatermark("event_time", WATERMARK_DELAY)
)

# ─────────────────────────────────────────────────────────────
# DEDUPLICATION
# ─────────────────────────────────────────────────────────────
deduped_stream = bronze_stream.dropDuplicates(["location_id", "event_time"])

# ─────────────────────────────────────────────────────────────
# CLEAN DATA
# ─────────────────────────────────────────────────────────────
silver_df = (
    deduped_stream

    # temperature sanity check
    .filter((col("temperature_2m") >= -50) & (col("temperature_2m") <= 60))

    # precipitation
    .filter((col("precipitation") >= 0) | col("precipitation").isNull())

    # humidity
    .filter(
        ((col("relative_humidity_2m") >= 0) &
         (col("relative_humidity_2m") <= 100)) |
        col("relative_humidity_2m").isNull()
    )

    # wind speed
    .filter((col("wind_speed_10m") >= 0) | col("wind_speed_10m").isNull())

    .select(
        "location_id",
        "event_time",

        "temperature_2m",
        "precipitation",
        "rain",
        "snowfall",
        "cloud_cover",
        "relative_humidity_2m",
        "surface_pressure",
        "wind_speed_10m",
        "wind_gusts_10m",
        "soil_temperature_0_to_7cm",
        "weather_code",

        "kafka_ts",
        col("ingest_time").alias("bronze_ingest_time")
    )

    .withColumn("silver_ingest_time", current_timestamp())

    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("day", dayofmonth(col("event_time")))
    .withColumn("hour", hour(col("event_time")))
)

# ─────────────────────────────────────────────────────────────
# WRITE STREAM
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

print("[INFO] ✅ Weather Clean streaming started")

query.awaitTermination()