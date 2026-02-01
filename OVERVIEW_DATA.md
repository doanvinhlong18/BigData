# 📊 Dữ Liệu Chuyến Đi FHV Khối Lượng Cao – Tổng Quan (NYC TLC)

## 1. Mô Tả Tập Dữ Liệu

Tập dữ liệu này chứa hồ sơ chuyến đi **Xe Thuê Khối Lượng Cao (HVFHV)** tại Thành phố New York.  
Mỗi hàng đại diện cho **một chuyến đi hoàn thành** được điều phối bởi các đơn vị FHV Khối Lượng Cao có giấy phép TLC, chẳng hạn như Uber, Lyft, Via hoặc Juno.

Dịch Vụ Xe Thuê Khối Lượng Cao (HVFHS) được định nghĩa chính thức theo **Luật Địa Phương 149 năm 2018**, có hiệu lực từ **ngày 1 tháng 2 năm 2019**, bao gồm các doanh nghiệp FHV điều phối hơn **10.000 chuyến đi mỗi ngày** dưới một thương hiệu duy nhất.

Dữ liệu được công bố bởi **Ủy Ban Taxi & Limousine NYC (TLC)**.

---

## 2. Phạm Vi Thời Gian

- **Khoảng thời gian:** Tháng 1/2025 – Tháng 11/2025  
- **Độ chi tiết:** Cấp độ từng chuyến đi  
- **Tệp tin:** 11 tệp Parquet theo tháng  
- **Định dạng:** Apache Parquet (lưu trữ theo cột)

---

## 3. Tóm Tắt Khối Lượng Dữ Liệu

| Tháng | Số Hàng (Chuyến Đi) | Nhóm Hàng |
|------|-------------|------------|
| T1 | 20.405.666 | 20 |
| T2 | 19.339.461 | 19 |
| T3 | 20.536.879 | 20 |
| T4 | 19.753.983 | 19 |
| T5 | 21.091.193 | 21 |
| T6 | 19.868.009 | 19 |
| T7 | 19.653.012 | 19 |
| T8 | 19.271.461 | 19 |
| T9 | 19.434.641 | 19 |
| T10 | 21.308.701 | 21 |
| T11 | 20.818.240 | 20 |

**Tổng số chuyến đi:** ~221 triệu  
**Tính nhất quán schema:** Giống hệt nhau qua tất cả các tháng

---

## 4. Tổng Quan Schema

Cả 11 tệp đều có cùng schema với **25 cột**.

### 4.1 Thông Tin Nhận Dạng & Cơ Sở

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| hvfhs_license_num | string | Giấy phép HVFHS (Uber, Lyft, Via, Juno) |
| dispatching_base_num | string | Cơ sở điều phối chuyến đi |
| originating_base_num | string | Cơ sở nhận yêu cầu ban đầu |

---

### 4.2 Trường Thời Gian

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| request_datetime | timestamp | Thời điểm hành khách yêu cầu đón |
| on_scene_datetime | timestamp | Thời điểm tài xế đến (chỉ AV) |
| pickup_datetime | timestamp | Thời điểm đón khách |
| dropoff_datetime | timestamp | Thời điểm trả khách |

---

### 4.3 Trường Không Gian

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| PULocationID | int | Khu vực Taxi TLC điểm đón |
| DOLocationID | int | Khu vực Taxi TLC điểm trả |

---

### 4.4 Chỉ Số Chuyến Đi

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| trip_miles | double | Khoảng cách chuyến đi (dặm) |
| trip_time | int | Thời lượng chuyến đi (giây) |

---

### 4.5 Trường Cước Phí & Tài Chính

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| base_passenger_fare | double | Cước phí cơ bản (trước phụ phí) |
| tolls | double | Phí cầu đường |
| bcf | double | Phí Quỹ Xe Đen |
| sales_tax | double | Thuế bán hàng tiểu bang NY |
| congestion_surcharge | double | Phụ phí tắc nghẽn NYC |
| airport_fee | double | Phí đón/trả sân bay |
| cbd_congestion_fee | double | Phí tắc nghẽn CBD (từ tháng 1/2025) |
| tips | double | Tiền boa hành khách |
| driver_pay | double | Thu nhập tài xế (không bao gồm boa & cầu đường) |

---

### 4.6 Cờ Đặc Điểm Chuyến Đi

| Trường | Kiểu | Mô Tả |
|-----|-----|------------|
| shared_request_flag | string | Hành khách đồng ý đi chung (Y/N) |
| shared_match_flag | string | Chuyến đi thực sự được chia sẻ (Y/N) |
| access_a_ride_flag | string | Chuyến đi MTA Access-A-Ride (Y/N) |
| wav_request_flag | string | Yêu cầu xe WAV (Y/N) |
| wav_match_flag | string | Xe WAV được cung cấp (Y/N) |

---

## 5. Siêu Dữ Liệu Kỹ Thuật

- **Phiên bản định dạng tệp:** Parquet 2.6  
- **Được tạo bởi:** parquet-cpp-arrow 16.1.0  
- **Kích thước nhóm hàng trung bình:** ~1 triệu hàng  
- **Bố cục lưu trữ:** Theo cột (tối ưu cho phân tích)

---

## 6. Đặc Điểm Chính & Ghi Chú

- Schema **hoàn toàn nhất quán** qua tất cả các tháng  
- Khối lượng dữ liệu **quá lớn để tải đầy đủ vào bộ nhớ** bằng pandas  
- Phù hợp cho:
  - Xử lý streaming / batch (PyArrow)
  - Phân tích SQL (DuckDB, Spark)
  - Phân tích chuỗi thời gian và không gian
- Bao gồm **các trường liên quan chính sách** như phí tắc nghẽn và sử dụng WAV

---

## 7. Hướng Phân Tích Được Đề Xuất

- Xu hướng khối lượng chuyến đi và doanh thu theo nhà cung cấp HVFHS  
- Mẫu thời gian (nhu cầu theo giờ, ngày, tháng)  
- Nhu cầu không gian theo Khu vực Taxi TLC  
- Tác động của phí tắc nghẽn và CBD  
- Thu nhập tài xế và hiệu quả chuyến đi  
- Khả năng tiếp cận và sử dụng chuyến đi chung

---

## 8. Công Cụ Được Đề Xuất

- **Khám phá:** PyArrow, DuckDB
- **Phân tích:** SQL, Pandas (sau khi tổng hợp), Spark
- **Trực quan hóa:** Matplotlib, Seaborn, Power BI, Tableau

---

*Tập dữ liệu này rất phù hợp cho phân tích di động đô thị quy mô lớn, tác động chính sách và kinh tế vận tải.*
## 9. Định Hướng Nghiên Cứu Được Lựa Chọn

### Phân Tích & Dự Báo Nhu Cầu Đặt Xe Dựa Trên Ngữ Cảnh (Context-Aware Ride Demand Analysis)

Nhóm nghiên cứu lựa chọn hướng nghiên cứu chính là:

**Phân tích và mô hình hóa nhu cầu đặt xe HVFHV dựa trên các yếu tố thời gian, không gian, kinh tế chuyến đi và điều kiện thời tiết.**

Mục tiêu là xây dựng cái nhìn toàn diện về hành vi nhu cầu di chuyển đô thị và xác định các yếu tố ảnh hưởng mạnh nhất đến khối lượng chuyến đi.

---

### 9.1 Mục Tiêu Nghiên Cứu

- Xác định các yếu tố ảnh hưởng đến số lượng chuyến đi HVFHV
- Phân tích sự thay đổi nhu cầu theo:
  - Thời gian (giờ, ngày, tháng)
  - Không gian (Taxi Zone – `LocationID`)
  - Đặc điểm chuyến đi (giá, quãng đường, thời gian)
  - Điều kiện thời tiết (nhiệt độ, mưa, tuyết, gió...)
- Xây dựng mô hình dự báo nhu cầu đặt xe theo khu vực và thời điểm

---

### 9.2 Các Nhóm Biến Phân Tích

#### 1. Biến Thời Gian

- `request_datetime`
- `pickup_datetime` → Trích xuất:
  - Giờ trong ngày
  - Ngày trong tuần
  - Cuối tuần / ngày thường
  - Tháng / mùa

#### 2. Biến Không Gian

- `PULocationID`
- `DOLocationID`

**Phân tích:**
- Mật độ nhu cầu theo khu vực
- Sự hình thành "mobility hotspot" theo thời gian

#### 3. Biến Kinh Tế & Chuyến Đi

- `base_passenger_fare`
- `trip_miles`
- `trip_time`
- `congestion_surcharge`
- `cbd_congestion_fee`
- `airport_fee`

**Nhằm đánh giá:**
- Giá và phụ phí có làm thay đổi nhu cầu không
- Khu vực có phí cao có giảm số chuyến đi không

#### 4. Biến Thời Tiết (từ dataset bổ sung)

Sau khi join với dataset thời tiết theo:
```
(LocationID + thời điểm theo giờ)
```

**Sử dụng các biến:**
- Nhiệt độ
- Lượng mưa
- Tuyết rơi
- Độ ẩm
- Gió
- Mây che phủ

**Để phân tích:**
- Mưa có làm tăng nhu cầu gọi xe không?
- Tuyết có làm giảm số chuyến đi không?
- Nhu cầu có nhạy cảm với thời tiết theo từng khu vực không?

---

### 9.3 Câu Hỏi Nghiên Cứu Chính

1. Nhu cầu đặt xe thay đổi như thế nào theo giờ và ngày trong tuần?
2. Những khu vực nào có nhu cầu cao ổn định theo thời gian?
3. Thời tiết ảnh hưởng đến nhu cầu ở mức độ nào?
4. Phí tắc nghẽn và phụ phí có làm thay đổi hành vi di chuyển không?
5. Có thể dự đoán số chuyến đi trong tương lai dựa trên:
   - Thời gian
   - Khu vực
   - Thời tiết
   - Chính sách phí

---

### 9.4 Bài Toán Học Máy Đề Xuất

| Bài toán | Mô tả |
|----------|-------|
| **Demand Forecasting** | Dự đoán số chuyến đi theo `LocationID` và giờ |
| **Feature Importance** | Xác định yếu tố ảnh hưởng mạnh nhất đến nhu cầu |
| **Spatial–Temporal Modeling** | Mô hình hóa nhu cầu theo không gian và thời gian |

**Các mô hình có thể sử dụng:**
- Regression (XGBoost, LightGBM)
- Time Series (LSTM, Prophet)
- Spatio-temporal models

---

### 9.5 Ý Nghĩa Nghiên Cứu

Hướng nghiên cứu này giúp:
- Hiểu rõ động lực nhu cầu vận tải đô thị
- Hỗ trợ hệ thống điều phối tài xế
- Cải thiện dự báo nhu cầu thời gian thực
- Đánh giá tác động của thời tiết và chính sách lên hành vi di chuyển