import os
from pyspark.sql import SparkSession
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
spark = SparkSession.builder.appName("test").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT).config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY).config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY).config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("delta").load("s3a://bronze/request")
df.select("request_datetime").show(5, False)
print("Total rows:", df.count())
print("Null request_datetime:", df.filter("request_datetime IS NULL").count())
