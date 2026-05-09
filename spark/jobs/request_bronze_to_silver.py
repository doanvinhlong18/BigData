"""
spark/jobs/request_bronze_to_silver.py
────────────────────────────────────────
Bronze sorted_request_table → Silver request (dedup + clean)

Passes ALL columns (không drop financials/flags).
Dedup theo trip_id trong watermark 15 phút.
"""

import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("request_bronze_to_silver")


def get_spark():
    ep = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    sk = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName("Request-Bronze-To-Silver")
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
    stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load("s3a://bronze/sorted_request_table")
        .withWatermark("request_datetime", "15 minutes")
        .filter(
            col("trip_id").isNotNull()
            & col("request_datetime").isNotNull()
            & col("PULocationID").isNotNull()
            & col("DOLocationID").isNotNull()
        )
        .filter(
            col("PULocationID").between(1, 265)
            & col("DOLocationID").between(1, 265)
            & (col("trip_miles").isNull() | col("trip_miles").between(0, 200))
            & (col("trip_time").isNull() | col("trip_time").between(0, 86400))
        )
        .drop("partition", "offset")  # Kafka metadata không cần downstream
        .withColumn("silver_ingest_time", current_timestamp())
    )

    deduped = stream.dropDuplicatesWithinWatermark(["trip_id"])

    (
        deduped.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3a://silver/checkpoints/request")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="1 second")
        .start("s3a://silver/request")
        .awaitTermination()
    )


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
