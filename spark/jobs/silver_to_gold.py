"""
spark/jobs/silver_to_gold.py
─────────────────────────────
Silver/complete + Silver/response → Gold/aggregated

Nguồn đọc (2 bảng):
  silver/complete  (stream trigger) → demand + dropoff + trip stats
  silver/response  (batch read)     → pickup_delay_mean/std, pickup_60m

Tách pickup_delay sang silver/response vì:
  - delay = pickup_datetime - request_datetime là chỉ số của giai đoạn response
    (request đã được đón), không nên bị lọc bởi điều kiện complete (phải có dropoff)
  - Dùng silver/response giúp tính delay trên toàn bộ trips đã pickup,
    kể cả những trip chưa kết thúc (chưa có dropoff)

Sliding window 60 phút, slide 15 phút.
Grain: (zone_id, window_end)
Upsert (MERGE) vì cùng window_end được tính lại khi late data đến.

Metrics:
  Từ silver/complete (via batch_df):
    - demand:  requests_60m, wav_req, aar_req, share_req, uber_req
               ← window request_datetime
    - dropoff: dropoff_60m, matched_rd
               ← window dropoff_datetime
    - trip:    avg_fare, avg_distance, avg_driver_pay, avg_tips, avg_trip_time,
               avg_bcf, avg_tolls, avg_congestion_surcharge, avg_airport_fee,
               avg_sales_tax, avg_cbd_congestion_fee
               ← window dropoff_datetime, trips_valid filter
  Từ silver/response (batch read, lọc theo window_end range):
    - pickup:  pickup_60m, pickup_delay_mean, pickup_delay_std,
               matched_rp, wav_match, share_match
               ← window pickup_datetime
  Tổng hợp:
    - neighbor: KHÔNG lưu — tính trong predict_service từ ZONE_NEIGHBORS (feature_builder)
    - imbalance: KHÔNG lưu — compute_imbalance() trong feature_builder tính từ snapshot
    - temporal: KHÔNG lưu — tính trong predict_service từ window_end (pandas nhanh hơn Spark)

TODO: Thêm weather features (temperature_2m, precipitation, snowfall, wind_speed_10m,
      wind_gusts_10m, relative_humidity_2m, surface_pressure, cloud_cover, weather_code,
      rain) từ silver/weather khi bảng đó có sẵn — join theo (zone_id, window_end).

Thay đổi so với phiên bản cũ:
  - Đổi tên wav_requests→wav_req, aar_requests→aar_req,
    shared_requests→share_req, uber_requests→uber_req (đồng bộ notebook)
  - Thêm avg_trip_time vào trip_stats
  - Thêm wav_match, share_match từ silver/response
  - Bỏ requests_15m, req_15m tumbling window (feature 15m đã loại bỏ)
  - Bỏ neighbor_count, neighbor_avg_requests_60m
  - Thêm neighbor_pickup_delay_mean (mean pickup_delay_mean của zone lân cận)
  [v2 - đồng bộ notebook]:
  - FIX matched_rp: đổi từ count("trip_id") → sum(request_datetime >= window.start)
  - FIX matched_rd: đổi từ count("trip_id") → sum(request_datetime >= window.start)
  - THÊM trip stats: avg_bcf, avg_tolls, avg_congestion_surcharge, avg_airport_fee,
                     avg_sales_tax, avg_cbd_congestion_fee
  - THÊM neighbor: num_neighbors
  - THÊM temporal features: is_weekend, is_holiday, slot_15m_sin/cos, dow_sin/cos,
                             hou_sin/cos, woy_sin/cos, mon_sin/cos
  [v3 - giảm tải streaming]:
  - BỎ add_temporal_features(): 13 cột temporal không lưu vào gold nữa
  - BỎ US_HOLIDAYS constant
  - BỎ import math (_PI2 không còn dùng)
  - Temporal features được tính trong predict_service bằng pandas (feature_builder._add_temporal)
    → nhanh hơn nhiều, không chiếm tài nguyên Spark micro-batch
  [v4 - chuyển nốt join sang predict]:
  - BỎ neighbor join + ZONE_NEIGHBORS dict — tính trong feature_builder
  - BỎ imbalance + ZONE_AREAS_KM2 import — compute_imbalance() đã có trong feature_builder
  - BỎ import sys, json (không còn dùng sau khi bỏ neighbor/imbalance)
  - Gold chỉ lưu window aggregate thuần: demand, pickup, dropoff, trip stats

ZONE_NEIGHBORS: dict hardcode 263 zones NYC.
"""

import os
import time
import traceback

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    lit,
    stddev,
    when,
    window,
)
from streaming_metrics import start_streaming_metrics_exporter

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_COMPLETE = "s3a://silver/complete"
SILVER_RESPONSE = "s3a://silver/response"  # dùng để tính pickup_delay
GOLD_AGG = "s3a://gold/aggregated"
GOLD_UPSERT_TMP_ROOT = os.getenv("GOLD_UPSERT_TMP_ROOT", "s3a://gold/_tmp").rstrip("/")
CHECKPOINT_ROOT = (
    os.getenv("STREAMING_CHECKPOINT_BASE") or "s3a://checkpoints"
).rstrip("/")
CHECKPOINT = f"{CHECKPOINT_ROOT}/gold/aggregated"
GOLD_MAX_FILES_PER_TRIGGER = os.getenv("GOLD_MAX_FILES_PER_TRIGGER", "10")

WINDOW_DURATION = os.getenv("GOLD_WINDOW_DURATION", "30 minutes")
SLIDE_DURATION = os.getenv("GOLD_SLIDE_DURATION", "5 minutes")
WINDOW_LOOKBACK_MINUTES = int(os.getenv("GOLD_WINDOW_LOOKBACK_MINUTES", "30"))
WATERMARK = "5 minutes"
TRIGGER_INTERVAL = f"{int(os.getenv('GOLD_TRIGGER_INTERVAL_S', os.getenv('SPARK_TRIGGER_INTERVAL_S', '60')))} seconds"
SOURCE_WAIT_POLL_S = int(os.getenv("SOURCE_WAIT_POLL_S", "15"))
SOURCE_WAIT_TIMEOUT_S = int(os.getenv("SOURCE_WAIT_TIMEOUT_S", "1800"))


def delete_path_if_exists(spark, path):
    """Xóa path tạm trên S3A nếu tồn tại."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    fs.delete(jpath, True)


def overwrite_upsert_preserving_history(spark, gold_df, batch_id):
    """
    Fallback khi Delta MERGE lỗi: ghi lại target bằng existing - keys + batch mới.

    Cách này đắt hơn MERGE nhưng vẫn giữ lịch sử window cũ, tránh lỗi cũ là
    overwrite toàn bộ gold bằng micro-batch hiện tại rồi làm latest window bị tụt.
    """
    tmp_path = f"{GOLD_UPSERT_TMP_ROOT}/aggregated_batch_{batch_id}_{int(time.time())}"
    keys = gold_df.select("zone_id", "window_end").distinct()

    existing = spark.read.format("delta").load(GOLD_AGG)
    merged = (
        existing.join(keys, on=["zone_id", "window_end"], how="left_anti")
        .unionByName(gold_df, allowMissingColumns=True)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        merged_count = merged.count()
        print(
            f"[gold] batch_id={batch_id} fallback_upsert_rows={merged_count}",
            flush=True,
        )
        delete_path_if_exists(spark, tmp_path)
        (
            merged.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(tmp_path)
        )
        (
            spark.read.format("delta")
            .load(tmp_path)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_AGG)
        )
    finally:
        merged.unpersist()
        delete_path_if_exists(spark, tmp_path)


def wait_for_source(spark, path, timeout=SOURCE_WAIT_TIMEOUT_S):
    """Chờ Delta table tồn tại trước khi readStream."""
    print(f"[wait_for_source] Chờ {path} ...", flush=True)
    elapsed = 0
    while elapsed < timeout:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                print(f"[wait_for_source] {path} sẵn sàng ({elapsed}s)", flush=True)
                return
        except Exception as e:
            print(f"[wait_for_source]   check error: {e}", flush=True)
        time.sleep(SOURCE_WAIT_POLL_S)
        elapsed += SOURCE_WAIT_POLL_S
        print(f"[wait_for_source]   ... {path} chưa sẵn sàng ({elapsed}s)", flush=True)
    raise TimeoutError(f"Source {path} không xuất hiện sau {timeout}s")


def main():
    spark = (
        SparkSession.builder.appName("silver_to_gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    start_streaming_metrics_exporter(spark, "silver_to_gold")
    print(
        "[gold] config "
        f"window={WINDOW_DURATION} slide={SLIDE_DURATION} "
        f"trigger={TRIGGER_INTERVAL} maxFilesPerTrigger={GOLD_MAX_FILES_PER_TRIGGER} "
        f"lookback={WINDOW_LOOKBACK_MINUTES}m checkpoint={CHECKPOINT}",
        flush=True,
    )

    # Chờ source sẵn sàng (jobs submit đồng thời)
    wait_for_source(spark, SILVER_COMPLETE)

    # ── Source: Silver/complete stream ────────────────────────────────────────
    # Watermark trên dropoff_datetime (event muộn nhất, luôn có) để drive window
    complete_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", GOLD_MAX_FILES_PER_TRIGGER)
        .load(SILVER_COMPLETE)
        .withWatermark("dropoff_datetime", WATERMARK)
    )

    def aggregate_and_upsert(batch_df, batch_id):
        started = time.time()
        print(f"[gold] batch_id={batch_id} START", flush=True)

        batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        demand_60m = None
        gold = None

        try:
            row_count = batch_df.count()
            print(f"[gold] batch_id={batch_id} input_rows={row_count}", flush=True)
            if row_count == 0:
                return

            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
            spark.conf.set("spark.sql.streaming.minBatchesToRetain", "10")

            # NEIGHBOR_JSON là biến global — dùng trực tiếp, không cần broadcast

            # ── 1. DEMAND metrics — groupBy PULocationID + window request_datetime ─
            # Tên cột đồng bộ notebook: wav_req, aar_req, share_req, uber_req
            # Bỏ requests_15m (feature 15m đã loại khỏi feature set)
            demand_60m = (
                batch_df.filter("PULocationID >= 1 AND PULocationID <= 263")
                .groupBy(
                    col("PULocationID").alias("zone_id"),
                    window("request_datetime", WINDOW_DURATION, SLIDE_DURATION).alias(
                        "w"
                    ),
                )
                .agg(
                    count("*").alias("requests_60m"),
                    count(when(col("wav_request_flag") == "Y", True)).alias("wav_req"),
                    count(when(col("access_a_ride_flag") == "Y", True)).alias(
                        "aar_req"
                    ),
                    count(when(col("shared_request_flag") == "Y", True)).alias(
                        "share_req"
                    ),
                    count(when(col("hvfhs_license_num") == "HV0003", True)).alias(
                        "uber_req"
                    ),
                )
                .select(
                    col("zone_id"),
                    col("w").getField("end").alias("window_end"),
                    "requests_60m",
                    "wav_req",
                    "aar_req",
                    "share_req",
                    "uber_req",
                )
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            # ── 2. PICKUP metrics — batch-read silver/response ────────────────────
            # pickup_delay = pickup_datetime - request_datetime là chỉ số của giai
            # đoạn response (request đã được đón), không phải complete (đã dropoff).
            # Đọc từ silver/response để tính delay trên TẤT CẢ trips đã pickup,
            # kể cả trip chưa có dropoff — tránh bias selection chỉ trip hoàn chỉnh.
            we_rows = demand_60m.select("window_end").distinct().collect()
            print(f"[gold] batch_id={batch_id} windows={len(we_rows)}", flush=True)
            if not we_rows:
                return
            min_we = min(r["window_end"] for r in we_rows)
            max_we = max(r["window_end"] for r in we_rows)

            response_df = (
                spark.read.format("delta")
                .load(SILVER_RESPONSE)
                .filter(
                    col("pickup_datetime").isNotNull()
                    & (
                        col("pickup_datetime")
                        >= F.lit(min_we)
                        - F.expr(f"INTERVAL {WINDOW_LOOKBACK_MINUTES} MINUTES")
                    )
                    & (col("pickup_datetime") < F.lit(max_we))
                )
                .select(
                    "trip_id",
                    "request_datetime",
                    "pickup_datetime",
                    "PULocationID",
                    "wav_match_flag",
                    "share_match_flag",
                )
            )

            pickup_metrics = (
                response_df.filter(
                    F.col("pickup_datetime") >= F.col("request_datetime")
                )
                .filter("PULocationID >= 1 AND PULocationID <= 263")
                # precompute delay
                .withColumn(
                    "pickup_delay",
                    F.col("pickup_datetime").cast("long")
                    - F.col("request_datetime").cast("long"),
                )
                .repartition("PULocationID")
                .withColumn(
                    "w", window("pickup_datetime", WINDOW_DURATION, SLIDE_DURATION)
                )
                .withColumn(
                    "matched_rp_flag",
                    F.when(
                        F.col("request_datetime") >= F.col("w").getField("start"),
                        F.lit(1),
                    ).otherwise(F.lit(0)),
                )
                .groupBy(
                    col("PULocationID").alias("zone_id"),
                    col("w"),
                )
                .agg(
                    count("*").alias("pickup_60m"),
                    avg("pickup_delay").alias("pickup_delay_mean"),
                    F.coalesce(stddev("pickup_delay"), F.lit(0)).alias(
                        "pickup_delay_std"
                    ),
                    # [FIX] matched_rp: đếm trips có request_datetime trong cùng window
                    # Đồng bộ notebook: sum(when(request_datetime >= window.start, 1))
                    F.sum("matched_rp_flag").alias("matched_rp"),
                    # wav_match / share_match từ silver/response
                    count(when(col("wav_match_flag") == "Y", True)).alias("wav_match"),
                    count(when(col("share_match_flag") == "Y", True)).alias(
                        "share_match"
                    ),
                )
                .select(
                    col("zone_id"),
                    col("w").getField("end").alias("window_end"),
                    "pickup_60m",
                    "pickup_delay_mean",
                    "pickup_delay_std",
                    "matched_rp",
                    "wav_match",
                    "share_match",
                )
            )

            # ── 3. DROPOFF metrics — groupBy DOLocationID + window dropoff_datetime ─
            dropoff_metrics = (
                batch_df.filter(col("dropoff_datetime").isNotNull())
                .filter("DOLocationID >= 1 AND DOLocationID <= 263")
                .repartition("DOLocationID")
                .withColumn(
                    "w", window("dropoff_datetime", WINDOW_DURATION, SLIDE_DURATION)
                )
                .withColumn(
                    "matched_rd_flag",
                    F.when(
                        F.col("request_datetime") >= F.col("w").getField("start"),
                        F.lit(1),
                    ).otherwise(F.lit(0)),
                )
                .groupBy(
                    col("DOLocationID").alias("zone_id"),
                    col("w"),
                )
                .agg(
                    count("*").alias("dropoff_60m"),
                    # [FIX] matched_rd: đếm trips có request_datetime trong cùng window
                    # Đồng bộ notebook: sum(when(request_datetime >= window.start, 1))
                    F.sum("matched_rd_flag").alias("matched_rd"),
                )
                .select(
                    col("zone_id"),
                    col("w").getField("end").alias("window_end"),
                    "dropoff_60m",
                    "matched_rd",
                )
            )

            # ── 4. TRIP STATS — groupBy PULocationID + window dropoff_datetime ──────
            # Filter trips_valid như notebook: trip_miles, trip_time, fare có giá trị hợp lệ
            # [THÊM] avg_bcf, avg_tolls, avg_congestion_surcharge, avg_airport_fee,
            #        avg_sales_tax, avg_cbd_congestion_fee — đồng bộ notebook dropoff_stats
            trip_stats = (
                batch_df.filter(
                    col("trip_miles").between(0.3, 500)
                    & col("trip_time").between(200, 20000)
                    & col("base_passenger_fare").between(1, 1500)
                    & col("driver_pay").between(1, 1500)
                )
                .filter("PULocationID >= 1 AND PULocationID <= 263")
                .groupBy(
                    col("PULocationID").alias("zone_id"),
                    window("dropoff_datetime", WINDOW_DURATION, SLIDE_DURATION).alias(
                        "w"
                    ),
                )
                .agg(
                    avg("trip_time").alias("avg_trip_time"),
                    avg("base_passenger_fare").alias("avg_fare"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("driver_pay").alias("avg_driver_pay"),
                    avg("tips").alias("avg_tips"),
                    # [THÊM] fee/surcharge features — đồng bộ notebook dropoff_stats
                    avg("bcf").alias("avg_bcf"),
                    avg("tolls").alias("avg_tolls"),
                    avg("congestion_surcharge").alias("avg_congestion_surcharge"),
                    avg("airport_fee").alias("avg_airport_fee"),
                    avg("sales_tax").alias("avg_sales_tax"),
                    avg("cbd_congestion_fee").alias("avg_cbd_congestion_fee"),
                )
                .select(
                    col("zone_id"),
                    col("w").getField("end").alias("window_end"),
                    "avg_trip_time",
                    "avg_fare",
                    "avg_distance",
                    "avg_driver_pay",
                    "avg_tips",
                    "avg_bcf",
                    "avg_tolls",
                    "avg_congestion_surcharge",
                    "avg_airport_fee",
                    "avg_sales_tax",
                    "avg_cbd_congestion_fee",
                )
            )

            # ── 5. Merge all metrics ──────────────────────────────────────────────
            gold = (
                demand_60m.join(
                    pickup_metrics, on=["zone_id", "window_end"], how="left"
                )
                .join(dropoff_metrics, on=["zone_id", "window_end"], how="left")
                .join(trip_stats, on=["zone_id", "window_end"], how="left")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            # ── 6. UPSERT vào gold/aggregated ────────────────────────────────────
            # Các features sau KHÔNG lưu vào gold — tính trong predict_service:
            #   - neighbor (requests_60m, pickup_delay_mean, num_neighbors)
            #     → feature_builder dùng ZONE_NEIGHBORS dict tĩnh, pandas merge nhanh
            #   - imbalance → compute_imbalance() trong feature_builder
            #   - temporal  → _add_temporal() trong feature_builder từ window_end
            gold_count = gold.count()
            print(f"[gold] batch_id={batch_id} gold_rows={gold_count}", flush=True)
            if gold_count == 0:
                return

            gold_key_count = gold.select("zone_id", "window_end").distinct().count()
            if gold_key_count != gold_count:
                print(
                    f"[gold] batch_id={batch_id} duplicate_keys="
                    f"{gold_count - gold_key_count}; dropDuplicates(zone_id, window_end)",
                    flush=True,
                )
                deduped_gold = gold.dropDuplicates(["zone_id", "window_end"]).persist(
                    StorageLevel.MEMORY_AND_DISK
                )
                gold.unpersist()
                gold = deduped_gold
                gold_count = gold.count()

            if DeltaTable.isDeltaTable(spark, GOLD_AGG):
                dt = DeltaTable.forPath(spark, GOLD_AGG)
                try:
                    (
                        dt.alias("old")
                        .merge(
                            gold.alias("new"),
                            "old.zone_id = new.zone_id AND old.window_end = new.window_end",
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    print(f"[gold] batch_id={batch_id} merge=OK", flush=True)
                except Exception as e:
                    print(
                        f"[gold] batch_id={batch_id} merge=FAILED "
                        f"{type(e).__name__}: {e}",
                        flush=True,
                    )
                    traceback.print_exc()
                    overwrite_upsert_preserving_history(spark, gold, batch_id)
            else:
                print(f"[gold] batch_id={batch_id} create {GOLD_AGG}", flush=True)
                gold.write.format("delta").mode("overwrite").save(GOLD_AGG)

            elapsed = time.time() - started
            print(f"[gold] batch_id={batch_id} DONE in {elapsed:.1f}s", flush=True)
        finally:
            if gold is not None:
                gold.unpersist()
            if demand_60m is not None:
                demand_60m.unpersist()
            batch_df.unpersist()

    # ── Streaming query ───────────────────────────────────────────────────────
    query = (
        complete_stream.writeStream.foreachBatch(aggregate_and_upsert)
        .queryName("silver_to_gold")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
