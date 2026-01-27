import pandas as pd

df = pd.read_parquet("datasets/fhvhv_tripdata_2025-02.parquet")

print(df.head())
print(df.info())