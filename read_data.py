import pyarrow.parquet as pq

df = pq.ParquetFile("datasets/response_table/response_2025_06.parquet")

print(df.schema_arrow)
