import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000").config("spark.hadoop.fs.s3a.access.key", "minioadmin").config("spark.hadoop.fs.s3a.secret.key", "minioadmin").config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("delta").load("s3a://bronze/request")
df.select("request_datetime").show(5, False)
print("Total rows:", df.count())
print("Null request_datetime:", df.filter("request_datetime IS NULL").count())
