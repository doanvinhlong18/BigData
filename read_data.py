import pandas as pd

df = pd.read_parquet("datasets/fhvhv_tripdata_2025-11.parquet")

print(df.head())
print(df.info())