# 🚕 NYC Green Taxi Trip Dataset

## 📋 Tổng quan

Bộ dữ liệu ghi nhận các chuyến đi của NYC Green Taxi năm 2018, bao gồm thông tin thời gian, vị trí, hành trình và chi phí. Dataset có kích thước lớn nên quá trình phân tích ban đầu được thực hiện bằng lấy mẫu dữ liệu (sampling) thay vì load toàn bộ vào bộ nhớ.

## 📊 Thông tin cơ bản

| Thuộc tính | Giá trị |
|------------|---------|
| **Dung lượng file trên ổ đĩa** | 826.63 MB |
| **Số cột** | 18 |
| **Phương pháp phân tích** | Đọc mẫu 10.000 dòng |
| **Năm dữ liệu** | 2018 |
| **Loại dữ liệu** | NYC Green Taxi Trip Records |

> ⚠️ **Lưu ý:** Dataset quá lớn để đọc toàn bộ bằng Pandas trên môi trường RAM hạn chế. Vì vậy chỉ sử dụng sampling để khám phá cấu trúc dữ liệu.

## 🗂️ Cấu trúc dữ liệu (từ mẫu 10.000 dòng)

### 🆔 Thông tin hệ thống

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `VendorID` | int64 | Mã nhà cung cấp thiết bị ghi nhận chuyến đi (1 = CMT, 2 = Verifone) |
| `store_and_fwd_flag` | object | Cờ cho biết dữ liệu có được lưu tạm do mất kết nối hay không (Y/N) |

### 🕒 Thông tin thời gian

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `lpep_pickup_datetime` | datetime64 | Thời điểm bắt đầu chuyến đi |
| `lpep_dropoff_datetime` | datetime64 | Thời điểm kết thúc chuyến đi |

> 📝 Dữ liệu gốc ở dạng chuỗi và đã được chuyển sang datetime.

### 📍 Thông tin vị trí

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `PULocationID` | int64 | Mã khu vực đón khách |
| `DOLocationID` | int64 | Mã khu vực trả khách |

### 🚗 Thông tin chuyến đi

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `passenger_count` | int64 | Số hành khách |
| `trip_distance` | float64 | Quãng đường (mile) |
| `trip_type` | int64 | Loại chuyến đi (1 = Street-hail, 2 = Dispatch) |
| `RatecodeID` | int64 | Mã loại giá cước |

### 💰 Chi phí & phụ phí

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `fare_amount` | float64 | Cước phí cơ bản |
| `extra` | int64 | Phụ phí |
| `mta_tax` | float64 | Thuế MTA |
| `improvement_surcharge` | float64 | Phí cải thiện |
| `tolls_amount` | int64 | Phí cầu đường |
| `total_amount` | float64 | Tổng chi phí |

### 💳 Thanh toán & tip

| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|--------------|-------|
| `tip_amount` | int64 | Tiền tip |
| `payment_type` | int64 | Hình thức thanh toán |

## 🧠 Phân loại kiểu dữ liệu

### Numeric (15 cột)
`VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `payment_type`, `trip_type`

### Categorical (1 cột)
`store_and_fwd_flag`

### Datetime (2 cột)
`lpep_pickup_datetime`, `lpep_dropoff_datetime`

---

## 📦 Cách sử dụng

### Load dữ liệu mẫu

```python
import pandas as pd

# Đọc mẫu 10,000 dòng
df = pd.read_csv('green_taxi_2018.csv', nrows=10000)

# Chuyển đổi cột datetime
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

# Xem thông tin cơ bản
print(df.info())
print(df.describe())
```

### Đọc dữ liệu theo chunk (xử lý file lớn)

```python
# Đọc từng chunk 50,000 dòng
chunk_size = 50000
for chunk in pd.read_csv('green_taxi_2018.csv', chunksize=chunk_size):
    # Xử lý từng chunk
    process_chunk(chunk)
```

### Sử dụng Dask cho big data

```python
import dask.dataframe as dd

# Đọc toàn bộ file với Dask
ddf = dd.read_csv('green_taxi_2018.csv')

# Thực hiện các phép tính
result = ddf.groupby('VendorID')['total_amount'].mean().compute()
```


---

## 🧠 Gợi ý phân tích & khai phá dữ liệu lớn

### 1️⃣ Phân tích thời gian (Temporal Analysis)

**Mục tiêu:** Tìm ra xu hướng theo thời gian, giờ cao điểm

**Cột sử dụng:**
- `lpep_pickup_datetime`, `lpep_dropoff_datetime`
- `trip_duration` (tính toán)

**Câu hỏi nghiên cứu:**
- Giờ nào trong ngày có nhiều chuyến đi nhất?
- Ngày nào trong tuần có nhu cầu cao nhất?
- Xu hướng theo tháng/quý như thế nào?
- Thời gian trung bình mỗi chuyến đi?

---

### 2️⃣ Phân tích không gian (Spatial Analysis)

**Mục tiêu:** Phân tích điểm nóng đón/trả khách

**Cột sử dụng:**
- `PULocationID`, `DOLocationID`
- `trip_distance`

**Câu hỏi nghiên cứu:**
- Khu vực nào có nhiều khách nhất?
- Khu vực nào có ít khách nhất?
- Tuyến đường phổ biến (OD matrix)?
- Quãng đường trung bình theo khu vực?

**Visualization:**
- Heatmap điểm đón/trả khách
- Network graph tuyến đường
- Choropleth map theo zone

---

### 3️⃣ Dự đoán giá cước (Fare Prediction)

**Mục tiêu:** Xây dựng mô hình dự đoán `total_amount`

**Features:**
- `trip_distance`, `trip_duration`
- `RatecodeID`, `PULocationID`, `DOLocationID`
- `pickup_hour`, `pickup_day`
- `passenger_count`
- `tolls_amount`, `extra`

**Mô hình gợi ý:**
- Linear Regression
- Random Forest
- XGBoost
- Neural Networks

**Metrics:**
- RMSE, MAE, R²

---

### 4️⃣ Phân tích hành vi khách hàng

**Mục tiêu:** Phân loại khách hàng, dự đoán tip

**Cột sử dụng:**
- `payment_type`, `tip_amount`
- `passenger_count`, `trip_type`

**Câu hỏi nghiên cứu:**
- Yếu tố nào ảnh hưởng đến tip?
- Người dùng thẻ vs tiền mặt khác nhau ra sao?
- Tỷ lệ tip theo giờ/khu vực?

**Mô hình:**
- Classification (tip > 0 hay không?)
- Regression (dự đoán tip_amount)

---

### 5️⃣ Clustering chuyến đi

**Mục tiêu:** Phân nhóm các loại chuyến đi

**Features:**
- `trip_distance`, `trip_duration`
- `fare_amount`, `total_amount`
- `pickup_hour`, `PULocationID`, `DOLocationID`

**Mô hình:**
- K-Means
- DBSCAN
- Hierarchical Clustering

**Ứng dụng:**
- Phân loại chuyến ngắn/dài
- Phân loại theo mục đích (sân bay, nội thành, ngoại ô)

---

### 6️⃣ Phát hiện bất thường (Anomaly Detection)

**Mục tiêu:** Tìm giao dịch bất thường, gian lận

**Indicators:**
- `fare_amount` quá cao/thấp so với `trip_distance`
- `trip_duration` bất thường
- `avg_speed` không hợp lý (<5 mph hoặc >80 mph)
- `tip_amount` > `fare_amount`

**Mô hình:**
- Isolation Forest
- One-Class SVM
- Autoencoders

---

## 📈 Kết quả mong đợi

### Insights kinh doanh
- Tối ưu hóa điều phối xe taxi
- Định giá động theo nhu cầu
- Dự đoán nhu cầu theo thời gian/khu vực
- Cải thiện trải nghiệm khách hàng

### Kỹ thuật Machine Learning
- Xây dựng hệ thống dự đoán giá
- Phát hiện gian lận tự động
- Khuyến nghị tuyến đường tối ưu
- Phân khúc khách hàng

---

## 🔗 Tài nguyên bổ sung

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Taxi Zone Lookup Table](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc)
- [Data Dictionary (Official)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)
- [Taxi Zone Shapefile](https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=Shapefile)

---

## 📝 Trích dẫn

Nếu sử dụng bộ dữ liệu này trong nghiên cứu, vui lòng trích dẫn:

```
NYC Taxi and Limousine Commission (TLC)
Green Taxi Trip Records - 2018
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Accessed: January 2026
```

---

## 📄 Giấy phép

Dữ liệu được cung cấp bởi NYC TLC theo chính sách dữ liệu mở của thành phố New York và có thể được sử dụng tự do cho mục đích nghiên cứu, phân tích và giáo dục.

---
