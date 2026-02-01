import pandas as pd

df = pd.read_parquet("datasets/request_table/request_2025_01.parquet")

print(df.head())
print(df.info())