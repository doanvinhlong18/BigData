# Phân tích và tinh gọn dashboard Grafana NYC Taxi

Ngày rà soát: 31/05/2026.

## Vấn đề đã phát hiện

- Zone map bị trắng dù Postgres có dữ liệu vì panel đang dùng static GeoJSON layer. Static GeoJSON của Grafana style chắc nhất theo `properties` nằm trong chính GeoJSON, còn dữ liệu prediction lại nằm ở query Postgres riêng. Cách join động `zone_id -> LocationID` không đáng tin cậy nếu không dùng Dynamic GeoJSON alpha.
- Grafana vẫn còn 5 dashboard vì SQLite volume của Grafana đang giữ 3 dashboard cũ: `Grafana metrics`, `Prometheus 2.0 Stats`, `Prometheus Stats`. Các dashboard này không nằm trong `grafana/dashboards`, nên sửa provisioning file không tự xóa chúng.
- Dashboard infrastructure không hiện rõ từng streaming job vì Spark jobs trước đó chưa có `.queryName(...)` và chưa export `StreamingQuery.lastProgress`. Metric Spark thô có tên khó đọc và không giống tab Structured Streaming ở Spark UI `4040`.

## Dashboard Demand Forecast

Map đã chuyển sang layer `Dynamic GeoJSON` để tự cập nhật màu theo query Postgres mỗi lần dashboard refresh. Basemap được đổi sang nền đen, tắt label nền, để các polygon NYC Taxi Zone và thang màu demand nổi rõ hơn.

Nguồn geometry vẫn là `http://localhost:8002/zone-predictions.geojson`, nhưng màu chính không còn phụ thuộc vào static style trong GeoJSON. Dashboard query latest prediction từ bảng `predictions_monitoring`, rồi layer `Dynamic GeoJSON` map `zone_id` trong query với `feature.id` trong GeoJSON.

Lỗi map "tối om" sau khi sửa lần đầu có 2 nguyên nhân:

- Container `predict-service` vẫn đang chạy cấu hình cũ, chưa publish port `8002` ra host và chưa có env `GEOJSON_PORT`. Trình duyệt không tải được `zone-predictions.geojson`, nên Geomap chỉ còn nền mặc định theo dark theme.
- Dashboard dùng các field style cũ như `fillColor`, `strokeColor`, `strokeWidth`. Với static GeoJSON layer của Grafana hiện tại, các field chắc chắn được dùng là `style.color`, `opacity`, `lineWidth` và `rules` theo feature property. Sau đó map được đổi tiếp sang `Dynamic GeoJSON` để màu tự refresh theo query Postgres, dùng nền đen và thêm layer viền trắng để từng TLC zone nhìn rõ hơn.

Endpoint này được predict service phục vụ bằng cách đọc `nyc_taxi_zones.geojson`, gán `feature.id = LocationID`, lấy latest prediction từ bảng `predictions_monitoring`, rồi ghi prediction vào `properties` của từng feature.

Mỗi zone có các property chính:

- `predicted_class`: cấp demand từ `0` đến `5`.
- `demand_label`: nhãn dễ đọc như `1 - Rất thấp`, `5 - Rất cao`.
- `confidence`: độ tự tin của model.
- `model`, `model_version`, `window_end`: metadata prediction.
- `fill`, `fill-opacity`, `stroke`, `stroke-width`: style tham khảo/debug nằm trong GeoJSON.

Grafana hiện tô màu polygon bằng `dataStyle.color.field = predicted_class` của `Dynamic GeoJSON`:

- Rule `predicted_class = 0`: xám rất nhạt.
- Rule `predicted_class = 1`: xanh nhạt.
- Rule `predicted_class = 2`: xanh rõ.
- Rule `predicted_class = 3`: vàng.
- Rule `predicted_class = 4`: cam.
- Rule `predicted_class = 5`: đỏ đậm.
- Zone không có prediction: tím.

Cơ chế tự cập nhật:

- Dashboard refresh mỗi `30s`.
- Mỗi lần refresh, query map lấy lại latest window trong Postgres.
- `Dynamic GeoJSON` dùng `zone_id` từ query để tìm đúng `feature.id` trong GeoJSON và đổi màu polygon.
- Endpoint GeoJSON vẫn có `Cache-Control: no-store`, nhưng geometry chủ yếu dùng làm nền không gian; màu prediction lấy từ query nên không cần reload toàn bộ trang.
- Query map có thêm một `Seed row` với `zone_id = -1` để tránh lỗi index `0` của plugin `Dynamic GeoJSON` không được style. Row này không khớp với feature nào nên không hiển thị trên bản đồ.

Lưu ý runtime lúc kiểm tra ngày 31/05/2026: bảng `predictions_monitoring` trong database `bigdata` đang có `0` dòng, nên map chỉ có nền đen và zone màu mặc định cho tới khi pipeline ghi lại prediction mới. Đây là vấn đề runtime, không phải lỗi style của map. Log mới nhất của `predict-service` cho thấy service không kết nối được tới Worker/MinIO `26.250.104.24:9000`, nên không đọc được `s3://gold/aggregated/_delta_log/`.

Các panel được giữ:

- `NYC Demand Forecast - Latest Zone Heatmap`: panel chính, trả lời trực tiếp model dự đoán demand ở từng địa điểm như thế nào.
- `Latest Event Window`: tránh nhầm event-time replay với wall-clock hiện tại.
- `Zones Covered`: số zone có prediction trong latest window.
- `Missing Zones`: số zone chưa có prediction, tính theo tổng `263` zone.
- `Average Confidence`: confidence trung bình của model.
- `Peak Demand Level`: mức demand cao nhất trong latest window.
- `Demand Distribution - Latest Window`: phân phối số zone theo demand class.
- `Demand Level Trend - Last 24h Event Time`: xu hướng theo event-time, không dùng `$__timeFilter(window_end)`.
- `Top Demand Zones - Latest Window`: danh sách vùng đáng chú ý nhất.

Các panel đã bỏ:

- `Model A / B Split`: latest window hiện chỉ có một model nên pie chart không có thêm insight.
- `All 263 Zones - Avg Demand (24h)`: bảng quá dài, không phù hợp demo và dễ gây hiểu nhầm giữa event-time với wall-clock.

## Dashboard Infrastructure & Pipeline Monitoring

Dashboard mới ưu tiên luồng vận hành: Kafka ingress -> Structured Streaming metrics -> prediction output.

Các health panel được giữ:

- `Kafka`: nguồn vào chính của pipeline.
- `Spark Master`: điều phối Spark applications.
- `Spark Workers`: số worker còn sống.
- `Spark Apps`: số app đang chạy.
- `MinIO`: nơi lưu bronze/silver/gold/checkpoints.
- `Postgres`: nơi dashboard forecast đọc prediction.
- `Predict Svc`: service ghi prediction vào Postgres.
- `Prod Models Loaded`: đếm số Production model đang được predict service load. Giá trị kỳ vọng là `2/2` vì pipeline đang dùng cả `model_a` và `model_b`; `1/2` nghĩa là chỉ một nhánh inference sẵn sàng, `0/2` nghĩa là service chưa có model để predict.

Các panel Kafka ingress:

- `Kafka Messages In/sec`: tốc độ message vào topic `nyc_taxi_events`.
- `Worker Host RAM Used %`: tỷ lệ RAM thật của worker host đang dùng, lấy từ node-exporter bằng `MemAvailable/MemTotal`. Metric này có ý nghĩa trực tiếp sau khi worker nâng lên 13GB vì cho biết cấu hình Spark còn headroom hay đã chạm vùng rủi ro.
- `Active Streaming Queries`: tổng số Structured Streaming query đang active theo `job_name/query`.
- `Prediction Freshness`: số phút từ lần predict gần nhất.

Các panel Structured Streaming giống Spark UI `4040`:

- `Input Rate by Job`: lấy từ `nyc_spark_stream_input_rows_per_second`, tương đương Input Rate trong Spark UI.
- `Process Rate by Job`: lấy từ `nyc_spark_stream_processed_rows_per_second`, tương đương Process Rate; dùng làm proxy throughput/output.
- `Input Rows per Batch`: lấy từ `nyc_spark_stream_input_rows`, số rows trong trigger gần nhất.
- `Batch Duration by Job`: lấy từ `nyc_spark_stream_batch_duration_ms`, tương đương `triggerExecution`.
- `Last Progress Age by Job`: lấy từ `nyc_spark_stream_last_progress_timestamp`, cho biết mỗi job đã bao lâu chưa có `lastProgress`. Đây là tín hiệu dễ đọc hơn operation duration: nếu tăng cao, job có thể đang kẹt, hết input hoặc exporter không cập nhật.
- `State Rows / Backlog`: lấy từ `nyc_spark_stream_state_rows_total`, hữu ích cho các job stream-stream join có state.

Các panel prediction output:

- `Prediction Confidence & Zone Coverage`: confidence trung bình và số zone có prediction theo event window.

Các panel đã bỏ:

- `MLflow`: `Model Loaded` trực tiếp hơn cho dashboard demo.
- `Airflow`: service Airflow đang bị comment trong compose và không đo streaming throughput.
- `Kafka Bytes In/Out per sec`: byte rate dễ bị trùng ý nghĩa với `Kafka Messages In/sec`, nhưng khó trả lời câu hỏi nghiệp vụ "pipeline đang nhận bao nhiêu event". Giữ message rate sẽ trực tiếp và dễ đọc hơn.
- `Operation Duration by Job`: quá nhiều series theo operation như `getOffset`, `addBatch`, `walCommit`; hữu ích khi debug Spark sâu nhưng rối cho dashboard tổng quan. Thay bằng `Last Progress Age by Job` để nhận biết job kẹt nhanh hơn.
- `Kafka Log End Offset`, `Kafka Request Latency p99`, `Kafka Log Size`, `Kafka JVM Heap Usage`: hữu ích khi debug Kafka sâu nhưng không kể câu chuyện input/output của pipeline.
- `Spark Driver Heap Usage`: resource metric chung, không giống Structured Streaming tab.
- `Host Resources`, `Container CPU/RAM`, `Network`, `Disk I/O`: nên để dashboard ops riêng nếu cần.
- `MinIO Storage Used`: MinIO health đủ cho demo pipeline; dung lượng storage không nói lên tốc độ job.

## Chia Lại Tài Nguyên Worker 13GB

Worker Spark hiện được cấu hình `SPARK_WORKER_MEMORY=13g` và `SPARK_WORKER_CORES=10`. Mục tiêu là dùng đủ 10 cores nhưng vẫn để khoảng 512MB buffer trong Spark worker container.

Phân bổ mới:

- `kafka_to_bronze`: `2048m + 384m overhead`, `2 cores`. Job này ingest trực tiếp từ Kafka và ghi nhiều nhánh bronze nên cần RAM/CPU hơn trước.
- `request_bronze_to_silver`: `768m + 256m overhead`, `1 core`. Job nhẹ, không join stateful nên giữ nhỏ để nhường tài nguyên.
- `request_to_response_silver`: `1280m + 384m overhead`, `2 cores`. Có stream-stream join nhỏ nên tăng core và shuffle partition để giảm nghẽn.
- `complete_bronze_to_silver`: `3072m + 512m overhead`, `3 cores`. Đây là job stateful nặng nhất nên được ưu tiên nhiều tài nguyên nhất.
- `silver_to_gold`: `3584m + 512m overhead`, `2 cores`. Job đọc silver/response và ghi gold/prediction, cần storage pool lớn nên giữ `spark.memory.storageFraction=0.5`.

Tổng executor slot là `12.5GB` và `10/10 cores`. Cấu hình này tận dụng worker mới tốt hơn cấu hình cũ `10.5GB`, nhưng vẫn không vượt `13GB`.

## Metric Model Loaded

Metric cũ `predict_service_model_loaded` chỉ có một giá trị `0/1`, nên dashboard không biết đang load được model nào. Predict service hiện export metric theo label:

- `predict_service_model_loaded{model="model_a", stage="Production"}`
- `predict_service_model_loaded{model="model_b", stage="Production"}`
- `predict_service_model_loaded{model="model_a", stage="Staging"}`

Dashboard dùng `sum(predict_service_model_loaded{stage="Production"})` để hiển thị `0/2`, `1/2`, hoặc `2/2`. Cách này có ý nghĩa hơn vì cho biết trạng thái ensemble Production, không chỉ biết "có ít nhất một model".

Các metric hỗ trợ thêm:

- `predict_service_model_ready`: `1` nếu có ít nhất một Production model để inference.
- `predict_service_models_loaded_total`: số Production model đang load.
- `predict_service_model_version{model, stage}`: version MLflow đang load, `0` nếu chưa load.

Lưu ý runtime lúc kiểm tra ngày 31/05/2026: metric `predict_service_model_version` đang thấy `model_a=1` và `model_b=1` trong MLflow registry, nhưng `predict_service_model_loaded` vẫn là `0/2` vì service chưa tải được artifact model từ MinIO. Log hiện tại là connect timeout tới `http://26.250.104.24:9000/mlflow-artifacts/.../MLmodel`. Nói cách khác, "đã register/promote model" khác với "predict-service đã download và load model vào RAM". Khi Worker/MinIO reachable và artifact tồn tại, panel sẽ tự chuyển sang `1/2` hoặc `2/2` sau lần reload model tiếp theo.

## Custom Metrics Mới

Mỗi Spark job gọi `start_streaming_metrics_exporter(...)` và được gán một `STREAMING_METRICS_PORT` riêng:

- `9101`: `kafka_to_bronze`
- `9102`: `request_bronze_to_silver`
- `9103`: `request_to_response_silver`
- `9104`: `complete_bronze_to_silver`
- `9105`: `silver_to_gold`

Prometheus scrape job mới: `spark-structured-streaming`.

Metric mới:

- `nyc_spark_stream_active`
- `nyc_spark_stream_input_rows`
- `nyc_spark_stream_input_rows_per_second`
- `nyc_spark_stream_processed_rows_per_second`
- `nyc_spark_stream_batch_duration_ms`
- `nyc_spark_stream_operation_duration_ms`
- `nyc_spark_stream_state_rows_total`
- `nyc_spark_stream_last_progress_timestamp`
- `nyc_spark_stream_exporter_up`

Giới hạn còn lại: `processedRowsPerSecond` là throughput xử lý của Spark, không phải số dòng đã commit thành công ở từng sink con. Nếu cần đếm riêng `bronze/request`, `bronze/pickup`, `bronze/dropoff`, cần thêm counter trong từng `foreachBatch`.
