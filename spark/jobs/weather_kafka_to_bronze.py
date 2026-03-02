"""
=============================================================
 BRONZE LAYER – WEATHER STREAMING
 Kafka topic: nyc_weather  →  MinIO Delta Table: s3a://bronze/weather/
=============================================================
 Run inside spark-streaming container:
   spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark/app/spark/jobs/weather_kafka_to_bronze.py
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
KAFKA_TOPIC       = "nyc_weather"           # phải khớp với weather_producer.py
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"

BRONZE_PATH       = "s3a://bronze/weather"
CHECKPOINT_PATH   = "s3a://bronze/checkpoints/weather"

# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Weather-Kafka-To-Bronze")

    # Delta Lake extensions
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # MinIO / S3A
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    # Performance tuning
    .config("spark.sql.streaming.schemaInference",        "true")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# SCHEMA – khớp chính xác với weather_producer.py
# ─────────────────────────────────────────────────────────────
weather_schema = StructType([
    StructField("location_id",              IntegerType(), True),
    StructField("datetime",                 StringType(),  True),   # ISO-8601 string
    StructField("temperature_2m",           DoubleType(),  True),
    StructField("precipitation",            DoubleType(),  True),
    StructField("rain",                     DoubleType(),  True),
    StructField("snowfall",                 DoubleType(),  True),
    StructField("cloud_cover",              DoubleType(),  True),
    StructField("relative_humidity_2m",     DoubleType(),  True),
    StructField("surface_pressure",         DoubleType(),  True),
    StructField("wind_speed_10m",           DoubleType(),  True),
    StructField("wind_gusts_10m",           DoubleType(),  True),
    StructField("soil_temperature_0_to_7cm",DoubleType(),  True),
    StructField("weather_code",             IntegerType(), True),
])

# ─────────────────────────────────────────────────────────────
# 1. ĐỌC TỪ KAFKA
# ─────────────────────────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")    # đổi thành "earliest" để test
    .option("failOnDataLoss",          "false")
    .load()
)

# ─────────────────────────────────────────────────────────────
# 2. PARSE JSON VALUE
# ─────────────────────────────────────────────────────────────
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str",
                "timestamp AS kafka_ts",        # Kafka ingestion time
                "partition",
                "offset")
    .select(
        from_json(col("json_str"), weather_schema).alias("data"),
        col("kafka_ts"),
        col("partition"),
        col("offset"),
    )
    .select("data.*", "kafka_ts", "partition", "offset")
)

# ─────────────────────────────────────────────────────────────
# 3. THÊM METADATA BRONZE (audit columns)
# ─────────────────────────────────────────────────────────────
bronze_df = (
    parsed_df
    .withColumn("event_time",   to_timestamp(col("datetime")))
    .withColumn("ingest_time",  current_timestamp())
    .withColumn("source_topic", lit(KAFKA_TOPIC))
    # Partition columns giúp query nhanh hơn ở Silver / Gold
    .withColumn("year",         year(col("event_time")))
    .withColumn("month",        month(col("event_time")))
    .withColumn("day",          dayofmonth(col("event_time")))
    # Bỏ cột gốc dạng string; đã có event_time timestamp
    .drop("datetime")
)

# ─────────────────────────────────────────────────────────────
# 4. GHI DELTA TABLE → MINIO BRONZE
# ─────────────────────────────────────────────────────────────
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    # Partition by year/month để tối ưu đọc tại tầng Silver
    .partitionBy("year", "month", "day")
    .trigger(processingTime="10 seconds")   # micro-batch mỗi 10s
    .start(BRONZE_PATH)
)

print(f"[INFO] Weather streaming started → {BRONZE_PATH}")
query.awaitTermination()