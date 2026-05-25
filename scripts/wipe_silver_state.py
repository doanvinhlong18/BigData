import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

spark = SparkSession.builder.appName("wipe_checkpoints").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT).config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY).config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY).config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()

sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
Path = sc._jvm.org.apache.hadoop.fs.Path

paths_to_delete = [
    "s3a://checkpoints/silver/response",
    "s3a://checkpoints/silver/complete",
    "s3a://checkpoints/gold/aggregated",
    "s3a://silver/response",
    "s3a://silver/complete",
    "s3a://gold/aggregated"
]

for p in paths_to_delete:
    try:
        path = Path(p)
        fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
        deleted = fs.delete(path, True)
        print(f"Deleted {p}: {deleted}")
    except Exception as e:
        print(f"Error deleting {p}: {e}")
