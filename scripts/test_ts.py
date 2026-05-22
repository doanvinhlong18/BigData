from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SELECT to_timestamp('2026-01-01T00:00:00+00:00') as ts").show()
