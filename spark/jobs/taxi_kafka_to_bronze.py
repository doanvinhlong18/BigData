"""
spark/jobs/taxi_kafka_to_bronze.py
────────────────────────────────────
Kafka → Bronze Delta (2 bảng, 2 schema thực sự khác nhau)

sorted_request_table schema:
  trip_id, hvfhs_license_num, dispatching_base_num, originating_base_num
  PULocationID, DOLocationID, request_datetime
  wav_request_flag, access_a_ride_flag, shared_request_flag
  + audit: ingest_time, kafka_ts, partition, offset

sorted_response_table schema:
  trip_id
  dropoff_datetime, pickup_datetime, on_scene_datetime
  trip_miles, trip_time
  base_passenger_fare, driver_pay, tips, tolls, bcf
  sales_tax, congestion_surcharge, airport_fee, cbd_congestion_fee
  shared_match_flag, wav_match_flag
  + audit: ingest_time, kafka_ts, partition, offset

Không có null fields kiểu superset — mỗi bảng chỉ lưu đúng những gì nó có.
"""

import os, logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    year,
    month,
    dayofmonth,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kafka_to_bronze")

# Schema chung để parse JSON — superset chỉ dùng khi parse, bỏ ngay sau đó
PARSE_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("hvfhs_license_num", StringType(), True),
        StructField("dispatching_base_num", StringType(), True),
        StructField("originating_base_num", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("request_datetime", StringType(), True),
        StructField("wav_request_flag", StringType(), True),
        StructField("access_a_ride_flag", StringType(), True),
        StructField("shared_request_flag", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("on_scene_datetime", StringType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", LongType(), True),
        StructField("base_passenger_fare", DoubleType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("bcf", DoubleType(), True),
        StructField("sales_tax", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True),
        StructField("shared_match_flag", StringType(), True),
        StructField("wav_match_flag", StringType(), True),
    ]
)

# Chỉ những columns thực sự có trong request
REQUEST_COLS = [
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
]

# Chỉ những columns thực sự có trong response
RESPONSE_COLS = [
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
]


def get_spark():
    ep = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    sk = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName("kafka-to-bronze")
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
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("delta.autoOptimize.optimizeWrite", "true")
        .getOrCreate()
    )


def run(spark):
    kb = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kb)
        .option("subscribe", "nyc_taxi_events")
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    parsed = (
        raw.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS json_str",
            "timestamp AS kafka_ts",
            "partition",
            "offset",
        )
        .select(
            from_json(col("json_str"), PARSE_SCHEMA).alias("d"),
            "kafka_key",
            "kafka_ts",
            "partition",
            "offset",
        )
        .select("d.*", "kafka_key", "kafka_ts", "partition", "offset")
        .filter(col("trip_id").isNotNull())
        .withColumn("ingest_time", current_timestamp())
    )

    def write_batch(batch_df: DataFrame, batch_id: int):
        batch_df.cache()

        # Request: chỉ SELECT đúng request columns, parse request_datetime
        req = (
            batch_df.filter(col("event_type") == "request")
            .select(REQUEST_COLS + ["ingest_time", "kafka_ts", "partition", "offset"])
            .withColumn("request_datetime", to_timestamp("request_datetime"))
            .filter(col("request_datetime").isNotNull())
            .withColumn("year", year("request_datetime"))
            .withColumn("month", month("request_datetime"))
            .withColumn("day", dayofmonth("request_datetime"))
        )

        # Response: chỉ SELECT đúng response columns, parse timestamps
        res = (
            batch_df.filter(col("event_type") == "response")
            .select(RESPONSE_COLS + ["ingest_time", "kafka_ts", "partition", "offset"])
            .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime"))
            .withColumn("pickup_datetime", to_timestamp("pickup_datetime"))
            .withColumn("on_scene_datetime", to_timestamp("on_scene_datetime"))
            .filter(
                col("dropoff_datetime").isNotNull() & col("pickup_datetime").isNotNull()
            )
            .withColumn("year", year("dropoff_datetime"))
            .withColumn("month", month("dropoff_datetime"))
            .withColumn("day", dayofmonth("dropoff_datetime"))
        )

        if not req.isEmpty():
            req.write.format("delta").mode("append").partitionBy(
                "year", "month", "day"
            ).save("s3a://bronze/sorted_request_table")
        if not res.isEmpty():
            res.write.format("delta").mode("append").partitionBy(
                "year", "month", "day"
            ).save("s3a://bronze/sorted_response_table")

        log.info(f"Batch {batch_id}: req={req.count():,} res={res.count():,}")
        batch_df.unpersist()

    (
        parsed.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", "s3a://bronze/checkpoints/taxi_bronze")
        .trigger(processingTime="2 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
