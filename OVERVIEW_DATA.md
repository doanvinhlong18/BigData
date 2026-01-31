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