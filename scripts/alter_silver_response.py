import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("alter_schema").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000").config("spark.hadoop.fs.s3a.access.key", "minioadmin").config("spark.hadoop.fs.s3a.secret.key", "minioadmin").config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()

try:
    spark.sql("ALTER TABLE delta.`s3a://silver/response` ADD COLUMNS (shared_match_flag STRING, wav_match_flag STRING)")
    print("Added columns to silver/response")
except Exception as e:
    print("Error:", e)
