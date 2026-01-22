# 🚕 NYC Green Taxi Trip Dataset

## 📋 Tổng quan

Bộ dữ liệu bao gồm các thuộc tính về thời gian, vị trí, hành trình và chi phí của mỗi chuyến đi taxi, cho phép thực hiện các bài toán phân tích dữ liệu lớn, khai phá dữ liệu và học máy trong bối cảnh giao thông đô thị.

### 📊 Thông tin cơ bản

| Thuộc tính                                  | Giá trị |
|---------------------------------------------|---------|
| **Số bản ghi**                              | 8,807,303 |
| **Số cột**                                  | 18 |
| **Dung lượng file gốc trên ổ đĩa**          | 826.63 MB |
| **Sử dụng bộ nhớ sau khi load bằng Pandas** | 2,838.96 MB|
| **Khoảng thời gian**                        | 2018 |
| **Loại dữ liệu**                            | Green Taxi Trip Records |

---

## 🗂️ Cấu trúc dữ liệu

### 🆔 Thông tin hệ thống

| Tên cột | Kiểu dữ liệu | Null Count | Mô tả |
|---------|--------------|------------|-------|
| `VendorID` | int64 | 0 | Mã nhà cung cấp thiết bị ghi nhận chuyến đi<br>• `1` = Creative Mobile Technologies<br>• `2` = Verifone Inc. |
| `store_and_fwd_flag` | object | 0 | Cờ lưu tạm dữ liệu trên xe do mất kết nối<br>• `N` = Không (8,790,612 chuyến)<br>• `Y` = Có (16,691 chuyến) |

---

### 🕒 Thông tin thời gian

| Tên cột | Kiểu dữ liệu | Null Count | Mô tả |
|---------|--------------|------------|-------|
| `lpep_pickup_datetime` | object | 0 | Thời điểm bắt đầu chuyến đi (đồng hồ tính tiền được bật) |
| `lpep_dropoff_datetime` | object | 0 | Thời điểm kết thúc chuyến đi (đồng hồ tính tiền được tắt) |

> **📌 Điều kiện hợp lệ:** `lpep_dropoff_datetime` > `lpep_pickup_datetime`

**Thống kê:**
- Số giá trị duy nhất (pickup): 7,410,484
- Số giá trị duy nhất (dropoff): 7,402,074
- Giá trị phổ biến nhất: 2018-04-18 11:11:39 (30 chuyến)

---

### 📍 Thông tin vị trí

| Tên cột | Kiểu dữ liệu | Null Count | Mean | Min | Max |
|---------|--------------|------------|------|-----|-----|
| `PULocationID` | int64 | 0 | 110.76 | 1 | 265 |
| `DOLocationID` | int64 | 0 | 128.80 | 1 | 265 |

> **📌 Lưu ý:** Có thể ánh xạ sang tên khu vực thông qua **Taxi Zone Lookup Table**

---

### 🚗 Thông tin chuyến đi

| Tên cột | Kiểu dữ liệu | Null Count | Mean | Min | Max | Mô tả |
|---------|--------------|------------|------|-----|-----|-------|
| `passenger_count` | int64 | 0 | 1.35 | 0 | 9 | Số lượng hành khách (do tài xế nhập) |
| `trip_distance` | float64 | 0 | 3.18 | 0.0 | 8,005.68 | Quãng đường (mile) |
| `trip_type` | float64 | 63 | 1.03 | 1.0 | 2.0 | `1` = Đón dọc đường<br>`2` = Điều phối |
| `RatecodeID` | int64 | 0 | 1.11 | 1 | 99 | Mã loại giá cước |

**Mã giá cước (RatecodeID):**
- `1` = Chuẩn
- `2` = JFK
- `3` = Newark
- `4` = Nassau/Westchester
- `5` = Thỏa thuận
- `6` = Đi chung

---

### 💰 Chi phí & phụ phí

| Tên cột | Kiểu dữ liệu | Null Count | Mean | Min | Max | Mô tả |
|---------|--------------|------------|------|-----|-----|-------|
| `fare_amount` | float64 | 0 | $13.43 | -$10,445.84 | $10,445.84 | Tiền cước cơ bản |
| `extra` | float64 | 0 | $0.33 | -$4.50 | $4.50 | Phụ phí giờ cao điểm/ban đêm |
| `mta_tax` | float64 | 0 | $0.49 | -$82.91 | $82.91 | Thuế MTA (chuẩn: $0.50) |
| `improvement_surcharge` | float64 | 0 | $0.29 | -$0.30 | $0.30 | Phụ phí cải thiện (chuẩn: $0.30) |
| `tolls_amount` | float64 | 0 | $0.17 | -$765.54 | $765.54 | Tổng phí cầu đường |

---

### 💳 Thanh toán & tiền tip

| Tên cột | Kiểu dữ liệu | Null Count | Mean | Min | Max | Mô tả |
|---------|--------------|------------|------|-----|-----|-------|
| `tip_amount` | float64 | 0 | $1.02 | -$485.55 | $485.55 | Tiền tip (chỉ qua thẻ) |
| `total_amount` | float64 | 0 | $15.75 | -$10,528.75 | $10,528.75 | Tổng tiền thanh toán |
| `payment_type` | int64 | 0 | 1.44 | 1 | 5 | Hình thức thanh toán |

**Hình thức thanh toán (payment_type):**
- `1` = Thẻ
- `2` = Tiền mặt
- `3` = Miễn phí
- `4` = Tranh chấp
- `5` = Không rõ
- `6` = Hủy chuyến

---

## ⚠️ Vấn đề chất lượng dữ liệu

### 🔍 Phát hiện dữ liệu bất thường

| Vấn đề | Số lượng | Tỷ lệ |
|--------|----------|-------|
| **Trip distance = 0** | 102,276 | 1.16% |
| **Fare amount ≤ 0** | 38,193 | 0.43% |
| **Passenger count ≤ 0** | 12,371 | 0.14% |
| **Trip type NULL** | 63 | <0.01% |

### 📌 Các vấn đề cần lưu ý

1. **Giá trị âm không hợp lý:**
   - `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount` có giá trị âm
   - Có thể do giao dịch hoàn tiền hoặc lỗi nhập liệu

2. **Giá trị ngoại lai (outliers):**
   - `trip_distance` max = 8,005.68 miles (bất thường)
   - `fare_amount` max = $10,445.84 (bất thường)
   - `mta_tax` max = $82.91 (chuẩn chỉ $0.50)

3. **Dữ liệu thiếu:**
   - `trip_type`: 63 giá trị NULL
   - Các cột khác: không có NULL

4. **Tip tiền mặt không được ghi nhận:**
   - `tip_amount` chỉ ghi nhận tip qua thẻ
   - Ảnh hưởng đến phân tích hành vi tip

5. **Passenger count do tài xế nhập:**
   - Có thể không chính xác tuyệt đối
   - Tồn tại 12,371 bản ghi có passenger_count ≤ 0

> **👉 Vì vậy, bước tiền xử lý dữ liệu là bắt buộc trước khi khai phá dữ liệu.**

---

## 🧹 Khuyến nghị tiền xử lý

### Bước 1: Xử lý dữ liệu thời gian
```python
# Chuyển đổi sang datetime
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

# Tính thời gian chuyến đi
df['trip_duration'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60

# Lọc chuyến đi hợp lệ (dropoff > pickup)
df = df[df['trip_duration'] > 0]
```

### Bước 2: Xử lý outliers và giá trị bất thường
```python
# Lọc trip_distance hợp lệ
df = df[(df['trip_distance'] > 0) & (df['trip_distance'] < 100)]

# Lọc fare_amount hợp lệ
df = df[(df['fare_amount'] > 0) & (df['fare_amount'] < 500)]

# Lọc passenger_count hợp lệ
df = df[(df['passenger_count'] > 0) & (df['passenger_count'] <= 6)]

# Lọc total_amount hợp lệ
df = df[(df['total_amount'] > 0) & (df['total_amount'] < 500)]
```

### Bước 3: Xử lý NULL
```python
# Điền giá trị NULL cho trip_type (nếu cần)
df['trip_type'].fillna(1, inplace=True)
```

### Bước 4: Tạo features mới
```python
# Trích xuất thời gian
df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour
df['pickup_day'] = df['lpep_pickup_datetime'].dt.day_name()
df['pickup_month'] = df['lpep_pickup_datetime'].dt.month

# Tính tốc độ trung bình (mph)
df['avg_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)

# Phân loại giờ cao điểm
df['is_rush_hour'] = df['pickup_hour'].apply(
    lambda x: 1 if (7 <= x <= 9) or (17 <= x <= 19) else 0
)
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
