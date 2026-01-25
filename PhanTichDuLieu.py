import pandas as pd
import numpy as np
import os

file_path = "datasets/nyc_taxi_2018.csv"
size_mb = os.path.getsize(file_path) / 1024**2

print(f"Dung lượng file trên disk: {size_mb:.2f} MB")

# ==============================
# 1. Load dataset
# ==============================
df = pd.read_csv("datasets/nyc_taxi_2018.csv")

print("=" * 80)
print("TỔNG QUAN BỘ DỮ LIỆU")
print("=" * 80)

print(f"Số bản ghi (rows): {len(df):,}")
print(f"Số cột (columns): {df.shape[1]}")
print("Không deep:", df.memory_usage().sum() / 1024**2, "MB")
print("Deep:", df.memory_usage(deep=True).sum() / 1024**2, "MB")

# ==============================
# 2. Danh sách cột & kiểu dữ liệu
# ==============================
print("\n" + "=" * 80)
print("DANH SÁCH CỘT & KIỂU DỮ LIỆU")
print("=" * 80)

col_info = pd.DataFrame(
    {
        "Column": df.columns,
        "Dtype": df.dtypes.values,
        "Non-Null Count": df.notnull().sum().values,
        "Null Count": df.isnull().sum().values,
        "Null Ratio (%)": (df.isnull().mean() * 100).round(2).values,
    }
)

print(col_info)

# ==============================
# 3. Phân loại cột theo kiểu dữ liệu
# ==============================
numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
datetime_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()

print("\n" + "=" * 80)
print("PHÂN LOẠI CỘT")
print("=" * 80)

print(f"Cột số (numeric): {numeric_cols}")
print(f"Cột phân loại (categorical): {categorical_cols}")
print(f"Cột thời gian (datetime): {datetime_cols}")

# ==============================
# 4. Thống kê mô tả các cột số
# ==============================
print("\n" + "=" * 80)
print("THỐNG KÊ MÔ TẢ CÁC CỘT SỐ")
print("=" * 80)

numeric_summary = df[numeric_cols].describe().T
numeric_summary["missing"] = df[numeric_cols].isnull().sum()
print(numeric_summary)

# ==============================
# 5. Thống kê các cột phân loại
# ==============================
print("\n" + "=" * 80)
print("THỐNG KÊ CÁC CỘT PHÂN LOẠI")
print("=" * 80)

for col in categorical_cols:
    print(f"\n--- {col} ---")
    print("Số giá trị duy nhất:", df[col].nunique(dropna=True))
    print("Top 5 giá trị phổ biến:")
    print(df[col].value_counts(dropna=False).head())

# ==============================
# 6. Phân tích các yếu tố quan trọng
# ==============================
print("\n" + "=" * 80)
print("CÁC YẾU TỐ QUAN TRỌNG CỦA BỘ DỮ LIỆU")
print("=" * 80)

important_features = {
    "Thời gian": ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    "Không gian": ["PULocationID", "DOLocationID"],
    "Hành vi khách hàng": ["passenger_count", "trip_distance", "trip_type"],
    "Tài chính": ["fare_amount", "tip_amount", "tolls_amount", "total_amount"],
    "Thanh toán": ["payment_type", "store_and_fwd_flag"],
}

for group, cols in important_features.items():
    print(f"\n📌 {group}:")
    for c in cols:
        if c in df.columns:
            print(f"  - {c}")

# ==============================
# 7. Kiểm tra nhanh dữ liệu bất thường
# ==============================
print("\n" + "=" * 80)
print("KIỂM TRA DỮ LIỆU BẤT THƯỜNG (SƠ BỘ)")
print("=" * 80)

if "trip_distance" in df.columns:
    print("Trip distance = 0:", (df["trip_distance"] == 0).sum())

if "fare_amount" in df.columns:
    print("Fare amount <= 0:", (df["fare_amount"] <= 0).sum())

if "passenger_count" in df.columns:
    print("Passenger count <= 0:", (df["passenger_count"] <= 0).sum())

print("\nHoàn tất mô tả dataset.")
