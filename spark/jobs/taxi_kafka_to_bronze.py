"""
=============================================================
 BRONZE LAYER – NYC TAXI STREAMING
 Kafka topic: nyc_taxi_events
   → event_type=request  → s3a://bronze/sorted_request_table/
   → event_type=response → s3a://bronze/sorted_response_table/
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType
)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "kafka:9092"
KAFKA_TOPIC       = "nyc_taxi_events"
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"

# ✅ 2 folder riêng trong bronze
REQUEST_PATH      = "s3a://bronze/sorted_request_table"
RESPONSE_PATH     = "s3a://bronze/sorted_response_table"
CHECKPOINT_REQ    = "s3a://bronze/checkpoints/sorted_request_table"
CHECKPOINT_RES    = "s3a://bronze/checkpoints/sorted_response_table"

# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TaxiEvents-Kafka-To-Bronze")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload",            "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer",     "bytebuffer")
    .config("spark.hadoop.fs.s3a.multipart.size",         "5M")
    .config("spark.hadoop.fs.s3a.block.size",             "32M")
    .config("spark.hadoop.fs.s3a.connection.maximum",     "100")
    .config("spark.hadoop.fs.s3a.threads.max",            "20")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
    .config("spark.databricks.delta.autoOptimize.autoCompact",   "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────
taxi_schema = StructType([
    StructField("event_type",           StringType(),  True),
    StructField("event_time",           StringType(),  True),
    StructField("trip_id",              StringType(),  True),
    StructField("hvfhs_license_num",    StringType(),  True),
    StructField("dispatching_base_num", StringType(),  True),
    StructField("pu_location_id",       IntegerType(), True),
    StructField("do_location_id",       IntegerType(), True),
    StructField("trip_miles",           DoubleType(),  True),
    StructField("trip_time",            IntegerType(), True),
    StructField("base_passenger_fare",  DoubleType(),  True),
    StructField("tips",                 DoubleType(),  True),
    StructField("tolls",                DoubleType(),  True),
    StructField("total_amount",         DoubleType(),  True),
])

# ─────────────────────────────────────────────────────────────
# 1. ĐỌC TRỰC TIẾP TỪ KAFKA
# ─────────────────────────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    .option("maxOffsetsPerTrigger",    "50000")
    .option("kafka.fetch.min.bytes",   "1")
    .option("kafka.fetch.max.wait.ms", "100")
    .option("kafka.max.poll.records",  "10000")
    .load()
)

# ─────────────────────────────────────────────────────────────
# 2. PARSE + METADATA
# ─────────────────────────────────────────────────────────────
parsed_df = (
    raw_df
    .selectExpr(
        "CAST(key AS STRING)   AS kafka_key",
        "CAST(value AS STRING) AS json_str",
        "timestamp             AS kafka_ts",
        "partition",
        "offset",
    )
    .select(
        from_json(col("json_str"), taxi_schema).alias("data"),
        col("kafka_key"), col("kafka_ts"), col("partition"), col("offset"),
    )
    .select("data.*", "kafka_key", "kafka_ts", "partition", "offset")
)

bronze_df = (
    parsed_df
    .withColumn("event_ts",     to_timestamp(col("event_time")))
    .withColumn("ingest_time",  current_timestamp())
    .withColumn("source_topic", lit(KAFKA_TOPIC))
    .withColumn("year",         year(col("event_ts")))
    .withColumn("month",        month(col("event_ts")))
    .withColumn("day",          dayofmonth(col("event_ts")))
    .drop("event_time")
)

# ─────────────────────────────────────────────────────────────
# 3. TÁCH request / response → GHI VÀO 2 FOLDER RIÊNG
# ─────────────────────────────────────────────────────────────
request_df  = bronze_df.filter(col("event_type") == "request")
response_df = bronze_df.filter(col("event_type") == "response")

query_request = (
    request_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_REQ)
    .partitionBy("year", "month", "day")
    .trigger(processingTime="1 second")
    .start(REQUEST_PATH)
)

query_response = (
    response_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_RES)
    .partitionBy("year", "month", "day")
    .trigger(processingTime="1 second")
    .start(RESPONSE_PATH)
)

print(f"[INFO] Request  stream → {REQUEST_PATH}")
print(f"[INFO] Response stream → {RESPONSE_PATH}")
print(f"[INFO] Pipeline: Kafka → Spark (1s) → MinIO bronze/")

spark.streams.awaitAnyTermination()
