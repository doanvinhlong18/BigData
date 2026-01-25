import pandas as pd
import os

file_path = "datasets/2018_taxi_trips.csv"

size_mb = os.path.getsize(file_path) / 1024**2
print(f"Dung lượng file trên disk: {size_mb:.2f} MB")

# Chỉ đọc cấu trúc cột
df_head = pd.read_csv(file_path, nrows=0)

print("\nCÁC CỘT TRONG DATASET:")
print(df_head.columns.tolist())
print(f"Số cột: {len(df_head.columns)}")

sample_df = pd.read_csv(file_path, nrows=10_000)

print("\n" + "="*80)
print("TỔNG QUAN TỪ MẪU 10,000 DÒNG")
print("="*80)

print(sample_df.head())
print("\nShape mẫu:", sample_df.shape)

print("\nKIỂU DỮ LIỆU:")
print(sample_df.dtypes)

numeric_cols = sample_df.select_dtypes(include=["int64", "float64"]).columns.tolist()
categorical_cols = sample_df.select_dtypes(include=["object"]).columns.tolist()

print("\nCột số:", numeric_cols)
print("Cột phân loại:", categorical_cols)

for col in sample_df.columns:
    if "datetime" in col:
        sample_df[col] = pd.to_datetime(sample_df[col], errors="coerce")

print("\nSau khi parse datetime:")
print(sample_df.dtypes)

