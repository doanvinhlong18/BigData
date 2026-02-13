"""
=============================================================
 BRONZE LAYER – NYC TAXI EVENTS STREAMING
 Kafka topic: nyc_taxi_events  →  MinIO Delta Table: s3a://bronze/taxi_events/
 Xử lý cả event_type=request và event_type=response trong một stream
=============================================================
 Run inside spark-streaming container:
   spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark/app/spark/jobs/taxi_kafka_to_bronze.py
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
KAFKA_TOPIC       = "nyc_taxi_events"       # khớp request_producer.py & response_producer.py
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"

BRONZE_PATH       = "s3a://bronze/taxi_events"
CHECKPOINT_PATH   = "s3a://bronze/checkpoints/taxi_events"

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

    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# SCHEMA – khớp chính xác với request_producer.py / response_producer.py
# Cả hai producer gửi cùng fields nhưng event_type khác nhau
# ─────────────────────────────────────────────────────────────
taxi_schema = StructType([
    StructField("event_type",           StringType(),  True),   # "request" | "response"
    StructField("event_time",           StringType(),  True),   # ISO-8601 string
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
# 1. ĐỌC TỪ KAFKA
# ─────────────────────────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    # Tăng throughput cho topic có nhiều message
    .option("maxOffsetsPerTrigger",    "50000")
    .load()
)

# ─────────────────────────────────────────────────────────────
# 2. PARSE JSON VALUE
# ─────────────────────────────────────────────────────────────
# Key là trip_id (bytes), decode sang string để lưu
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
        col("kafka_key"),
        col("kafka_ts"),
        col("partition"),
        col("offset"),
    )
    .select("data.*", "kafka_key", "kafka_ts", "partition", "offset")
)

# ─────────────────────────────────────────────────────────────
# 3. THÊM METADATA BRONZE
# ─────────────────────────────────────────────────────────────
bronze_df = (
    parsed_df
    .withColumn("event_ts",     to_timestamp(col("event_time")))
    .withColumn("ingest_time",  current_timestamp())
    .withColumn("source_topic", lit(KAFKA_TOPIC))
    .withColumn("year",         year(col("event_ts")))
    .withColumn("month",        month(col("event_ts")))
    .withColumn("day",          dayofmonth(col("event_ts")))
    .drop("event_time")     # đã có event_ts dạng timestamp
)

# ─────────────────────────────────────────────────────────────
# 4. GHI DELTA TABLE → MINIO BRONZE
#    Partition by event_type để query request/response riêng biệt
# ─────────────────────────────────────────────────────────────
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("year", "month", "event_type")
    .trigger(processingTime="10 seconds")
    .start(BRONZE_PATH)
)

print(f"[INFO] Taxi events streaming started → {BRONZE_PATH}")
query.awaitTermination()