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
    - trip:    avg_fare, avg_distance, avg_driver_pay, avg_tips, avg_trip_time
               ← window dropoff_datetime, trips_valid filter
  Từ silver/response (batch read, lọc theo window_end range):
    - pickup:  pickup_60m, pickup_delay_mean, pickup_delay_std,
               matched_rp, wav_match, share_match
               ← window pickup_datetime
  Tổng hợp:
    - neighbor: neighbor_requests_60m, neighbor_pickup_delay_mean

Thay đổi so với phiên bản cũ:
  - Đổi tên wav_requests→wav_req, aar_requests→aar_req,
    shared_requests→share_req, uber_requests→uber_req (đồng bộ notebook)
  - Thêm avg_trip_time vào trip_stats
  - Thêm wav_match, share_match từ silver/response
  - Bỏ requests_15m, req_15m tumbling window (feature 15m đã loại bỏ)
  - Bỏ neighbor_count, neighbor_avg_requests_60m
  - Thêm neighbor_pickup_delay_mean (mean pickup_delay_mean của zone lân cận)

ZONE_NEIGHBORS: dict hardcode 263 zones NYC.
"""

import json
import os
import sys

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    lit,
    stddev,
    sum as spark_sum,
    when,
    window,
)

# ZONE_AREAS_KM2 dùng để tính imbalance = (delay^1.2 × requests) / zone_area
# Import từ feature_builder (single source of truth)
sys.path.insert(0, "/opt/ml")
try:
    from feature_builder import ZONE_AREAS_KM2
except ImportError:
    # Fallback: default 1.0 km² cho tất cả zones nếu feature_builder không có
    ZONE_AREAS_KM2 = {i: 1.0 for i in range(1, 264)}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_COMPLETE = "s3a://silver/complete"
SILVER_RESPONSE = "s3a://silver/response"  # dùng để tính pickup_delay
GOLD_AGG = "s3a://gold/aggregated"
CHECKPOINT = "s3a://checkpoints/gold/aggregated"

WINDOW_DURATION = "60 minutes"
SLIDE_DURATION = "15 minutes"
WATERMARK = "15 minutes"
TRIGGER_INTERVAL = "15 seconds"

ZONE_NEIGHBORS = {
    1: [2, 3, 4],
    2: [1, 7, 8, 30],
    3: [1, 4, 5, 32],
    4: [1, 3, 79, 224],
    5: [99, 84, 109],
    6: [221, 214],
    7: [2, 8, 179, 193],
    8: [7, 179, 223],
    9: [16, 73, 192],
    10: [216, 218],
    11: [21, 22, 67],
    12: [13, 88],
    13: [12, 88, 261],
    14: [67, 227],
    15: [171, 252],
    16: [9, 64, 175],
    17: [49, 225],
    18: [136, 241],
    19: [64, 101],
    20: [31, 18],
    21: [11, 22, 123],
    22: [11, 21, 67, 123],
    23: [156, 187],
    24: [41, 151],
    25: [97, 65],
    26: [133, 227],
    27: [201],
    28: [130, 134],
    29: [150, 210],
    30: [2],
    31: [20, 32],
    32: [3, 31, 174],
    33: [65, 66],
    34: [66, 217],
    35: [77, 72],
    36: [37, 80],
    37: [36, 225],
    38: [139, 205],
    39: [91, 155],
    40: [106, 195],
    41: [24, 166],
    42: [152, 116],
    43: [236, 239],
    44: [204],
    45: [232, 209],
    46: [],
    47: [59, 169],
    48: [50, 163],
    49: [17, 97],
    50: [48, 142],
    51: [81, 184],
    52: [54],
    53: [252],
    54: [52, 33],
    55: [108],
    56: [82, 95],
    57: [173],
    58: [183],
    59: [47, 60],
    60: [59, 78],
    61: [62, 189],
    62: [61, 188],
    63: [177],
    64: [16, 19],
    65: [25, 33],
    66: [33, 34],
    67: [11, 14, 22],
    68: [100, 246],
    69: [247, 119],
    70: [129, 173],
    71: [85, 72],
    72: [71, 188],
    73: [9, 171],
    74: [41, 75],
    75: [74, 236],
    76: [77],
    77: [35, 76],
    78: [60, 20],
    79: [4, 107],
    80: [36, 255],
    81: [51, 254],
    82: [56, 83],
    83: [82, 260],
    84: [5, 109],
    85: [71, 89],
    86: [117],
    87: [209, 261],
    88: [12, 13],
    89: [85, 165],
    90: [234, 186],
    91: [39, 165],
    92: [171, 253],
    93: [95, 135],
    94: [136, 169],
    95: [56, 93],
    96: [102, 198],
    97: [25, 49],
    98: [121, 175],
    99: [5, 118],
    100: [68, 230],
    101: [19, 64],
    102: [96, 95],
    103: [],
    104: [],
    105: [],
    106: [40, 181],
    107: [79, 137],
    108: [55, 123],
    109: [5, 84, 110],
    110: [109, 176],
    111: [190, 227],
    112: [255],
    113: [114, 249],
    114: [113, 125],
    115: [221, 245],
    116: [42, 152],
    117: [86, 201],
    118: [99, 109],
    119: [69],
    120: [244],
    121: [98, 135],
    122: [191, 131],
    123: [21, 22, 108],
    124: [180],
    125: [114, 158],
    126: [147, 168],
    127: [128, 243],
    128: [127, 153],
    129: [70, 207],
    130: [28, 134],
    131: [122, 98],
    132: [219],
    133: [26, 111],
    134: [28, 130],
    135: [93, 121],
    136: [18, 94],
    137: [107, 170],
    138: [223, 207],
    139: [38, 203],
    140: [141, 202],
    141: [140, 237],
    142: [143, 50],
    143: [142, 239],
    144: [148, 211],
    145: [193, 226],
    146: [193, 7],
    147: [126, 159],
    148: [144, 232],
    149: [155, 123],
    150: [29],
    151: [24, 238],
    152: [42, 166],
    153: [128],
    154: [],
    155: [39, 149],
    156: [23],
    157: [160, 226],
    158: [125, 249],
    159: [147, 168],
    160: [157, 196],
    161: [162, 230],
    162: [161, 229],
    163: [48, 230],
    164: [186, 170],
    165: [89, 91],
    166: [41, 152],
    167: [60, 69],
    168: [126, 159],
    169: [47, 94],
    170: [137, 164],
    171: [15, 73, 92],
    172: [214, 176],
    173: [70, 57],
    174: [32, 240],
    175: [16, 98],
    176: [110, 172],
    177: [63],
    178: [165],
    179: [7, 8],
    180: [124],
    181: [106, 190],
    182: [248],
    183: [58],
    184: [51],
    185: [3],
    186: [90, 164],
    187: [23],
    188: [62, 72],
    189: [61],
    190: [111, 181],
    191: [122],
    192: [9],
    193: [145, 146],
    194: [],
    195: [40],
    196: [160],
    197: [134],
    198: [96],
    199: [],
    200: [240],
    201: [27, 117],
    202: [140],
    203: [139],
    204: [44],
    205: [38],
    206: [245],
    207: [129, 138],
    208: [],
    209: [45, 87],
    210: [29],
    211: [144],
    212: [248],
    213: [],
    214: [6, 172],
    215: [218],
    216: [10],
    217: [34],
    218: [10, 215],
    219: [132],
    220: [200],
    221: [6, 115],
    222: [],
    223: [8, 138],
    224: [4],
    225: [17, 37],
    226: [145, 157],
    227: [14, 26, 111],
    228: [],
    229: [162],
    230: [100, 161, 163],
    231: [211],
    232: [45, 148],
    233: [170],
    234: [90],
    235: [],
    236: [43, 75],
    237: [141],
    238: [151],
    239: [43, 143],
    240: [174, 200],
    241: [18],
    242: [],
    243: [127],
    244: [120],
    245: [115, 206],
    246: [68],
    247: [69],
    248: [182, 212],
    249: [113, 158],
    250: [],
    251: [],
    252: [15, 53],
    253: [92],
    254: [81],
    255: [80, 112],
    256: [],
    257: [],
    258: [],
    259: [],
    260: [83],
    261: [13, 87],
    262: [],
    263: [],
}

NEIGHBOR_JSON = json.dumps(ZONE_NEIGHBORS)


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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Source: Silver/complete stream ────────────────────────────────────────
    # Watermark trên dropoff_datetime (event muộn nhất, luôn có) để drive window
    complete_stream = (
        spark.readStream.format("delta")
        .load(SILVER_COMPLETE)
        .withWatermark("dropoff_datetime", WATERMARK)
    )

    def aggregate_and_upsert(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # NEIGHBOR_JSON là biến global — dùng trực tiếp, không cần broadcast

        # ── 1. DEMAND metrics — groupBy PULocationID + window request_datetime ─
        # Tên cột đồng bộ notebook: wav_req, aar_req, share_req, uber_req
        # Bỏ requests_15m (feature 15m đã loại khỏi feature set)
        demand_60m = (
            batch_df.filter("PULocationID >= 1 AND PULocationID <= 263")
            .groupBy(
                col("PULocationID").alias("zone_id"),
                window("request_datetime", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            )
            .agg(
                count("*").alias("requests_60m"),
                count(when(col("wav_request_flag") == "Y", True)).alias("wav_req"),
                count(when(col("access_a_ride_flag") == "Y", True)).alias("aar_req"),
                count(when(col("shared_request_flag") == "Y", True)).alias("share_req"),
                count(when(col("hvfhs_license_num") == "HV0003", True)).alias("uber_req"),
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
        )

        # ── 2. PICKUP metrics — batch-read silver/response ────────────────────
        # pickup_delay = pickup_datetime - request_datetime là chỉ số của giai
        # đoạn response (request đã được đón), không phải complete (đã dropoff).
        # Đọc từ silver/response để tính delay trên TẤT CẢ trips đã pickup,
        # kể cả trip chưa có dropoff — tránh bias selection chỉ trip hoàn chỉnh.
        we_rows = demand_60m.select("window_end").distinct().collect()
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
                    >= F.lit(min_we) - F.expr("INTERVAL 60 MINUTES")
                )
                & (col("pickup_datetime") < F.lit(max_we))
            )
        )

        pickup_metrics = (
            response_df.filter(F.col("pickup_datetime") >= F.col("request_datetime"))
            .filter("PULocationID >= 1 AND PULocationID <= 263")
            # precompute delay
            .withColumn(
                "pickup_delay",
                F.col("pickup_datetime").cast("long")
                - F.col("request_datetime").cast("long"),
            )
            .groupBy(
                col("PULocationID").alias("zone_id"),
                window("pickup_datetime", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            )
            .agg(
                count("*").alias("pickup_60m"),
                count("trip_id").alias("matched_rp"),
                avg("pickup_delay").alias("pickup_delay_mean"),
                F.coalesce(stddev("pickup_delay"), F.lit(0)).alias("pickup_delay_std"),
                # wav_match / share_match từ silver/response (có wav_match_flag, share_match_flag)
                count(when(col("wav_match_flag") == "Y", True)).alias("wav_match"),
                count(when(col("share_match_flag") == "Y", True)).alias("share_match"),
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
            .groupBy(
                col("DOLocationID").alias("zone_id"),
                window("dropoff_datetime", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            )
            .agg(
                count("*").alias("dropoff_60m"),
                count("trip_id").alias("matched_rd"),
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
                window("dropoff_datetime", WINDOW_DURATION, SLIDE_DURATION).alias("w"),
            )
            .agg(
                avg("trip_time").alias("avg_trip_time"),
                avg("base_passenger_fare").alias("avg_fare"),
                avg("trip_miles").alias("avg_distance"),
                avg("driver_pay").alias("avg_driver_pay"),
                avg("tips").alias("avg_tips"),
            )
            .select(
                col("zone_id"),
                col("w").getField("end").alias("window_end"),
                "avg_trip_time",
                "avg_fare",
                "avg_distance",
                "avg_driver_pay",
                "avg_tips",
            )
        )

        # ── 5. Merge all metrics ──────────────────────────────────────────────
        gold = (
            demand_60m.join(pickup_metrics, on=["zone_id", "window_end"], how="left")
            .join(dropoff_metrics, on=["zone_id", "window_end"], how="left")
            .join(trip_stats, on=["zone_id", "window_end"], how="left")
        )

        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

        # ── Build adjacency DataFrame ────────────────────────────────────────
        neighbor_map = json.loads(NEIGHBOR_JSON)

        adj_rows = [
            (int(zone), int(n))
            for zone, neighbors in neighbor_map.items()
            for n in neighbors
        ]

        adj_schema = StructType(
            [
                StructField("zone_id", IntegerType(), False),
                StructField("neighbor_zone_id", IntegerType(), False),
            ]
        )

        adj_df = spark.createDataFrame(adj_rows, adj_schema)

        # ── Compute neighbor features ─────────────────────────────────────────
        # neighbor_requests_60m     : tổng requests_60m của zone lân cận
        # neighbor_pickup_delay_mean: mean pickup_delay_mean của zone lân cận
        # Đồng bộ notebook cell 21 compute_neighbor_requests()
        neighbor_base = demand_60m.select(
            "zone_id", "window_end", "requests_60m"
        ).join(
            pickup_metrics.select("zone_id", "window_end", "pickup_delay_mean"),
            on=["zone_id", "window_end"],
            how="left",
        )

        neighbor_feat = (
            neighbor_base.alias("d")
            .join(
                F.broadcast(adj_df).alias("a"),
                F.col("d.zone_id") == F.col("a.zone_id"),
                "left",
            )
            .join(
                neighbor_base.alias("n"),
                (F.col("a.neighbor_zone_id") == F.col("n.zone_id"))
                & (F.col("d.window_end") == F.col("n.window_end")),
                "left",
            )
            .groupBy("d.zone_id", "d.window_end")
            .agg(
                F.sum("n.requests_60m").alias("neighbor_requests_60m"),
                F.mean("n.pickup_delay_mean").alias("neighbor_pickup_delay_mean"),
            )
        )

        # ── Join vào gold ───────────────────────────────────────────────────
        gold = gold.join(neighbor_feat, ["zone_id", "window_end"], "left")

        gold = gold.withColumn(
            "neighbor_requests_60m",
            F.coalesce(F.col("neighbor_requests_60m"), F.lit(0.0)),
        ).withColumn(
            "neighbor_pickup_delay_mean",
            F.coalesce(F.col("neighbor_pickup_delay_mean"), F.lit(0.0)),
        )

        # ── 7. IMBALANCE = (pickup_delay_mean^1.2 × requests_60m) / zone_area ─
        # Đồng bộ feature_builder.compute_imbalance() và notebook cell 25
        # Broadcast zone_area map để tránh shuffle
        zone_area_rows = [(int(z), float(a)) for z, a in ZONE_AREAS_KM2.items()]
        zone_area_df = spark.createDataFrame(
            zone_area_rows,
            StructType([
                StructField("zone_id", IntegerType(), False),
                StructField("zone_area_km2", DoubleType(), False),
            ])
        )
        gold = gold.join(F.broadcast(zone_area_df), on="zone_id", how="left")
        gold = gold.withColumn(
            "imbalance",
            F.when(
                F.col("pickup_delay_mean").isNotNull() & F.col("requests_60m").isNotNull(),
                (F.col("pickup_delay_mean").cast("double") ** 1.2)
                * F.col("requests_60m")
                / F.coalesce(F.col("zone_area_km2"), F.lit(1.0))
            ).otherwise(F.lit(0.0))
        ).drop("zone_area_km2")

        # ── 8. UPSERT vào gold/aggregated ────────────────────────────────────
        if gold.isEmpty():
            return

        try:
            dt = DeltaTable.forPath(spark, GOLD_AGG)
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
        except Exception:
            # Bảng chưa tồn tại → tạo mới
            gold.write.format("delta").mode("overwrite").save(GOLD_AGG)

    # ── Streaming query ───────────────────────────────────────────────────────
    query = (
        complete_stream.writeStream.foreachBatch(aggregate_and_upsert)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
