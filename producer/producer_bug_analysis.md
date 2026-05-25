# Phân tích lỗi Replay Stream chậm (Unified Producer)

Báo cáo phân tích nguyên nhân stream chạy chậm hơn kỳ vọng dù đã đặt `SPEED_FACTOR=60`.

## 1. Xác định Replay Clock đang hoạt động thế nào
- **Replay theo field nào?**: Code đang replay động theo `time_field` được định nghĩa trong `CONFIG` (ví dụ: `request_datetime`, `pickup_datetime`, `dropoff_datetime` tùy thuộc vào mode truyền vào).
- **Cách tính clock**: Đồng hồ dựa trên khoảng cách thời gian (delta) giữa sự kiện hiện tại (`cur`) và sự kiện liền trước (`prev_time`). Không có Global Wall-clock (đồng hồ tổng).
- **Field dùng để sleep**: Chính là kết quả parse của `time_field` thông qua hàm `_parse_time(row.get(tf))`.

## 2. Trace toàn bộ logic sleep/delay
- **Hàm thực hiện sleep**: Dùng `time.sleep(d / SPEED_FACTOR)` tại dòng `150` trong hàm `_sleep(prev, curr)`. Đây là blocking sleep (làm dừng toàn bộ thread của producer).
- **Công thức tính delta**: `d = (curr - prev).total_seconds()` tại dòng `148`.
- **Đơn vị tính**: `total_seconds()` trả về **Seconds (Giây)**.
- **Áp dụng Speed Factor**: Phép tính `d / SPEED_FACTOR` là **ĐÚNG về mặt toán học**. Nếu khoảng cách là 3600 giây (1 giờ), chia cho `SPEED_FACTOR=60` sẽ ra 60 giây (1 phút thực tế). 
- **Điều kiện lọc**: Lệnh sleep chỉ chạy nếu `0 < d < 3600` (dòng 149). Tức là nếu event bị lùi thời gian (`d < 0`) hoặc văng quá xa (`d > 3600`) thì bỏ qua sleep.

## 3. Kiểm tra dataframe/event ordering
- **Sorting**: Code sử dụng `pq.ParquetFile(file_path).iter_batches()` (dòng 160). Hàm này đọc tuần tự raw data từ file Parquet **KHÔNG HỀ CÓ BƯỚC SORT**.
- **Multi-thread/Kafka Partitions**: Bản thân producer script chạy single-thread. Dù có ném vào Kafka nhiều partition thì việc đọc file và sleep vẫn bị thắt cổ chai ở 1 thread duy nhất này.
- **Ordering của Dataset (Nguyên nhân cốt lõi)**: File Parquet gốc của TLC NYC Taxi thường được sort theo `pickup_datetime`. 
  - Nghĩa là nếu chạy `pickup-producer`, dữ liệu gần như theo thứ tự.
  - **NHƯNG** nếu chạy `dropoff-producer`, trường `dropoff_datetime` sẽ **bị xáo trộn nghiêm trọng (out-of-order)** vì thời gian mỗi chuyến đi (trip duration) là khác nhau. Một chuyến đi ngắn xuất phát sau có thể kết thúc trước một chuyến đi dài xuất phát trước.

## 4. Tìm bug logic phổ biến
Bug chí mạng nằm ở cơ chế **Replay reset clock liên tục** do không quản lý được High-water mark (mốc thời gian lớn nhất). 

Khi dữ liệu bị out-of-order (ví dụ event hiện tại ở quá khứ so với event trước), hàm `_sleep` sẽ thấy `d < 0` và không sleep. Điều này hoàn toàn đúng.
Nhưng ở dòng `178`, code lại gán:
```python
prev_time = cur
```
Việc này vô tình **kéo ngược mốc thời gian của đồng hồ replay về quá khứ**. Khi vòng lặp đọc event tiếp theo (trở lại tương lai), delta `d` lại sinh ra một số dương khổng lồ, khiến chương trình lại thực hiện `time.sleep()`.

Kết quả: Chương trình **cộng dồn tất cả các khoảng thời gian dương** trong một chuỗi zigzag, khiến tổng thời gian sleep phình to gấp hàng chục lần so với thực tế của timeline.

## 5. Ví dụ thực tế từ logic code
Hãy xem xét 3 trips nằm kề nhau trong file Parquet (đã được sort theo pickup):
* **Trip A**: Pickup `08:00`, Dropoff **`08:30`**
* **Trip B**: Pickup `08:05`, Dropoff **`08:15`** (Chuyến đi ngắn)
* **Trip C**: Pickup `08:06`, Dropoff **`08:35`**

Khi `dropoff-producer` chạy qua 3 trips này:
1. Đọc Trip A (`cur = 08:30`). Giả sử `prev = 08:00`. Delta `d` = 30 phút. **Actual Sleep = 30s**. Code gán `prev_time = 08:30`.
2. Đọc Trip B (`cur = 08:15`). So với `prev = 08:30`, delta `d = -15` phút. Lệnh `_sleep` return sớm (không sleep). Nhưng code gán lùi `prev_time = 08:15`.
3. Đọc Trip C (`cur = 08:35`). So với `prev = 08:15`, delta `d = 20` phút. **Actual Sleep = 20s**. Code gán `prev_time = 08:35`.

**Phân tích kết quả:**
- Theo timeline thực tế (Global Clock), từ Trip A đến Trip C chỉ mất **5 phút** (`08:30` đến `08:35`).
- Expected Sleep (thời gian thực tế cần chờ) = 5 phút / 60 = **5 giây**.
- **Actual Sleep (thời gian code thực sự chờ)** = 30s + 20s = **50 giây** (Gấp 10 lần thời gian thực!!!). 
Càng nhiều chuyến đi chéo nhau, độ lệch này càng nhân lên khổng lồ.

## 6. Cách Fix Bug
**Vị trí Bug:**
- File: `producer/unified_producer.py`
- Line: `178` (trong hàm `read_and_send`)
- Code lỗi: `prev_time = cur`

**Cách giải quyết (Giữ nguyên kiến trúc, fix bằng mốc High-water mark):**
Chỉ được phép tịnh tiến `prev_time` về tương lai, không được lùi về quá khứ.
Sửa dòng `178` từ:
```python
prev_time = cur
```
Thành:
```python
prev_time = max(prev_time, cur) if prev_time else cur
```

## 7. Kết luận cuối cùng
* **Nguyên nhân chính**: Việc gán lùi `prev_time` khi gặp dữ liệu out-of-order làm phá vỡ hoàn toàn replay clock, biến các độ trễ âm thành các khoảng sleep dương khổng lồ.
* **Mức độ ảnh hưởng**: Cực kỳ nghiêm trọng. Nó giải thích chính xác tại sao dataset 1 giờ lại tốn tới gần 10 phút để chạy thay vì 1 phút như công thức `SPEED_FACTOR=60`. Đặc biệt producer của `dropoff` và `request` sẽ là nạn nhân nặng nề nhất.
* **Cách fix tốt nhất**: Thay vì load toàn bộ file vào RAM để sort (tốn tài nguyên), áp dụng chiến thuật **High-water mark** (`prev_time = max(...)`) như ở trên là giải pháp O(1) hiệu quả, thanh lịch và chuẩn nhất cho hệ thống streaming simulation.
