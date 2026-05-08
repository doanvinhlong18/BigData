"""
spark/jobs/silver_to_gold.py
──────────────────────────────
Silver complete → Gold aggregated

Gold schema (grain: zone_id × window_end):
  Demand, Pickup, Dropoff, Stats, Neighbor, Weather (10 cols)

Lý do lưu weather vào Gold:
  - Window + join (shuffle-heavy) → lưu để tránh tính lại
  - Weather join là lookup nhỏ (263 rows) → cùng lúc, không tốn thêm
  - Predict DAG chỉ đọc Gold, không cần đọc CSV riêng
  - Lag weather suy ra từ Gold history (rẻ, no shuffle)
  - Lead weather (T+15, T+30, T+45) → đọc silver/weather 3 rows, inject nhỏ

KHÔNG lưu trong Gold (suy ra khi cần, rẻ):
  - imbalance          = pickup_delay_mean^1.2 × requests_60m
  - temporal features  = suy ra từ window_end (sin/cos)
  - lag features       = shift() trên 263×N pandas rows
"""

import os, logging, math
from datetime import datetime, timedelta, timezone
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, TimestampType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("silver_to_gold")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_COMPLETE = os.getenv("SILVER_COMPLETE", "s3a://silver/complete")
SILVER_WEATHER = os.getenv("SILVER_WEATHER", "s3a://silver/weather")
GOLD_AGGREGATED = os.getenv("GOLD_AGGREGATED", "s3a://gold/aggregated")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "s3a://checkpoints/silver_to_gold")

WINDOW_SECONDS = 60 * 60
SLIDE_SECONDS = 15 * 60
WATERMARK_MIN = 15

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


def create_spark():
    return (
        SparkSession.builder.appName("silver-to-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )


def flag(c):
    return F.when(F.col(c) == "Y", 1).otherwise(0)


def is_uber():
    return F.when(F.col("hvfhs_license_num") == "HV0003", 1).otherwise(0)


def sliding(ts):
    return F.window(F.col(ts), f"{WINDOW_SECONDS} seconds", f"{SLIDE_SECONDS} seconds")


def compute_neighbor_features(demand_df, spark):
    adj_rows = [(z, n) for z, ns in ZONE_NEIGHBORS.items() for n in ns]
    adj = spark.createDataFrame(
        adj_rows,
        schema=StructType(
            [
                StructField("zone_id", IntegerType(), False),
                StructField("nbr_id", IntegerType(), False),
            ]
        ),
    )
    focal = demand_df.join(adj, "zone_id", "left").select(
        demand_df["zone_id"].alias("fz"), F.col("nbr_id"), demand_df["window_end"]
    )
    return (
        focal.join(
            demand_df.alias("n"),
            (focal["nbr_id"] == F.col("n.zone_id"))
            & (focal["window_end"] == F.col("n.window_end")),
            "left",
        )
        .groupBy("fz", focal["window_end"])
        .agg(
            F.sum("n.requests_60m").alias("neighbor_requests_60m"),
            F.sum("n.requests_15m").alias("neighbor_requests_15m"),
            F.avg("n.pickup_delay_mean").alias("neighbor_pickup_delay_mean"),
            F.count("nbr_id").alias("num_neighbors"),
        )
        .withColumnRenamed("fz", "zone_id")
    )


def build_gold_for_windows(spark, window_ends):
    if not window_ends:
        return None
    min_we = min(window_ends)
    max_we = max(window_ends)
    read_from = min_we - timedelta(seconds=WINDOW_SECONDS + 1800)

    raw = (
        spark.read.format("delta")
        .load(SILVER_COMPLETE)
        .filter(
            (F.col("request_datetime") >= F.lit(read_from))
            | (F.col("pickup_datetime") >= F.lit(read_from))
            | (F.col("dropoff_datetime") >= F.lit(read_from))
        )
    )

    trips = raw.filter(
        (F.col("base_passenger_fare") > 0)
        & (F.col("driver_pay") > 0)
        & (F.col("trip_miles") < 500)
        & (F.col("trip_time") < 20000)
        & (F.col("driver_pay") < 1500)
        & (F.col("pickup_datetime") > F.col("request_datetime"))
    )

    trips_valid = trips.filter(
        F.col("trip_miles").between(0.3, 500)
        & F.col("trip_time").between(200, 20000)
        & F.col("base_passenger_fare").between(1, 1500)
        & F.col("driver_pay").between(1, 1500)
    )

    SLIDE_EXPR = F.expr(f"INTERVAL {SLIDE_SECONDS} SECONDS")

    def in_range(df):
        return df.filter(
            (F.col("zone_id") <= 263)
            & (F.col("window_end") >= F.lit(min_we))
            & (F.col("window_end") <= F.lit(max_we))
        )

    demand = in_range(
        trips.withColumn("zone_id", F.col("PULocationID").cast("int"))
        .withColumn("w", sliding("request_datetime"))
        .groupBy("zone_id", "w")
        .agg(
            F.count("*").alias("requests_60m"),
            F.sum(
                F.when(
                    F.col("request_datetime") >= F.col("w.end") - SLIDE_EXPR, 1
                ).otherwise(0)
            ).alias("requests_15m"),
            F.sum(flag("wav_request_flag")).alias("wav_req"),
            F.sum(flag("access_a_ride_flag")).alias("aar_req"),
            F.sum(flag("shared_request_flag")).alias("share_req"),
            F.sum(is_uber()).alias("uber_req"),
        )
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    pickup = in_range(
        trips.withColumn("zone_id", F.col("PULocationID").cast("int"))
        .withColumn("w", sliding("pickup_datetime"))
        .withColumn(
            "pickup_delay_s",
            F.unix_timestamp("pickup_datetime") - F.unix_timestamp("request_datetime"),
        )
        .groupBy("zone_id", "w")
        .agg(
            F.count("*").alias("pickup_60m"),
            F.sum(
                F.when(
                    F.col("pickup_datetime") >= F.col("w.end") - SLIDE_EXPR, 1
                ).otherwise(0)
            ).alias("pickup_15m"),
            F.mean("pickup_delay_s").alias("pickup_delay_mean"),
            F.stddev("pickup_delay_s").alias("pickup_delay_std"),
            F.sum(
                F.when(F.col("request_datetime") >= F.col("w.start"), 1).otherwise(0)
            ).alias("matched_rp"),
            F.sum(
                F.when(
                    (F.col("request_datetime") >= F.col("w.start"))
                    & (F.col("pickup_datetime") >= F.col("w.end") - SLIDE_EXPR),
                    1,
                ).otherwise(0)
            ).alias("matched_rp_15m"),
            F.sum(flag("wav_match_flag")).alias("wav_match"),
            F.sum(flag("shared_match_flag")).alias("share_match"),
        )
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    dc = in_range(
        trips.withColumn("zone_id", F.col("DOLocationID").cast("int"))
        .withColumn("w", sliding("dropoff_datetime"))
        .groupBy("zone_id", "w")
        .agg(
            F.count("*").alias("dropoff_60m"),
            F.sum(
                F.when(
                    F.col("dropoff_datetime") >= F.col("w.end") - SLIDE_EXPR, 1
                ).otherwise(0)
            ).alias("dropoff_15m"),
            F.sum(
                F.when(F.col("request_datetime") >= F.col("w.start"), 1).otherwise(0)
            ).alias("matched_rd"),
        )
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    ds = in_range(
        trips_valid.withColumn("zone_id", F.col("PULocationID").cast("int"))
        .withColumn("w", sliding("dropoff_datetime"))
        .groupBy("zone_id", "w")
        .agg(
            F.mean("trip_time").alias("avg_trip_time"),
            F.mean("base_passenger_fare").alias("avg_fare"),
            F.mean("driver_pay").alias("avg_driver_pay"),
            F.mean("tips").alias("avg_tips"),
            F.mean("bcf").alias("avg_bcf"),
            F.mean("tolls").alias("avg_tolls"),
            F.mean("congestion_surcharge").alias("avg_congestion_surcharge"),
            F.mean("airport_fee").alias("avg_airport_fee"),
            F.mean("sales_tax").alias("avg_sales_tax"),
            F.mean("cbd_congestion_fee").alias("avg_cbd_congestion_fee"),
            F.mean("trip_miles").alias("avg_distance"),
        )
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    # full_grid
    zdf = spark.createDataFrame([(z,) for z in range(1, 264)], ["zone_id"])
    tdf = spark.createDataFrame(
        [(we,) for we in window_ends], ["window_end"]
    ).withColumn("window_end", F.col("window_end").cast(TimestampType()))
    fg = zdf.crossJoin(tdf)

    gold = (
        fg.join(demand, ["zone_id", "window_end"], "left")
        .join(pickup, ["zone_id", "window_end"], "left")
        .join(
            dc.withColumnRenamed("zone_id", "_dz"),
            (fg["zone_id"] == F.col("_dz")) & (fg["window_end"] == dc["window_end"]),
            "left",
        )
        .drop("_dz", dc["window_end"])
        .join(ds, ["zone_id", "window_end"], "left")
    )

    COUNT_COLS = [
        "requests_60m",
        "requests_15m",
        "pickup_60m",
        "pickup_15m",
        "dropoff_60m",
        "dropoff_15m",
        "matched_rp",
        "matched_rp_15m",
        "matched_rd",
        "wav_req",
        "aar_req",
        "share_req",
        "uber_req",
        "wav_match",
        "share_match",
    ]
    gold = gold.fillna(0, subset=[c for c in COUNT_COLS if c in gold.columns])

    # Neighbor features
    nbr = compute_neighbor_features(
        gold.select(
            "zone_id", "window_end", "requests_60m", "requests_15m", "pickup_delay_mean"
        ),
        spark,
    )
    gold = gold.join(nbr, ["zone_id", "window_end"], "left")

    # ── Weather join từ silver/weather ────────────────────────────────────────
    # Lý do join ở đây (không ở Airflow):
    #   - Join xảy ra 1 lần khi window đóng, không lặp lại
    #   - silver/weather đã được load từ 2526.csv (load_weather_to_silver.py)
    #   - Predict DAG đọc Gold trực tiếp, có sẵn weather
    #   - Lag weather (lag1..lag4) suy ra từ Gold history, không cần CSV lại
    #   - Chỉ lead weather (T+15/30/45) cần đọc thêm 3 rows từ silver/weather
    try:
        weather = (
            spark.read.format("delta")
            .load(SILVER_WEATHER)
            .select(
                "zone_id",
                "window_end",
                "temperature_2m",
                "relative_humidity_2m",
                "surface_pressure",
                "precipitation",
                "rain",
                "snowfall",
                "cloud_cover",
                "weather_code",
                "wind_speed_10m",
                "wind_gusts_10m",
            )
        )
        gold = gold.join(weather, ["zone_id", "window_end"], "left")
        log.info("Weather joined into Gold")
    except Exception as e:
        log.warning(f"silver/weather not available: {e} — weather cols = NULL")

    # Partition
    gold = (
        gold.withColumn("year", F.year("window_end"))
        .withColumn("month", F.month("window_end"))
        .withColumn("day", F.dayofmonth("window_end"))
    )
    return gold


def merge_into_gold(spark, batch):
    if DeltaTable.isDeltaTable(spark, GOLD_AGGREGATED):
        (
            DeltaTable.forPath(spark, GOLD_AGGREGATED)
            .alias("e")
            .merge(
                batch.alias("n"), "e.zone_id=n.zone_id AND e.window_end=n.window_end"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch.write.format("delta").partitionBy("year", "month", "day").mode(
            "overwrite"
        ).save(GOLD_AGGREGATED)


def process_batch(micro_df, batch_id, spark):
    if micro_df.isEmpty():
        return
    max_ts = micro_df.select(
        F.greatest(
            F.max("request_datetime"),
            F.max("pickup_datetime"),
            F.max("dropoff_datetime"),
        ).alias("m")
    ).first()["m"]
    if not max_ts:
        return
    if hasattr(max_ts, "astimezone"):
        max_ts = max_ts.astimezone(timezone.utc).replace(tzinfo=None)
    wc = max_ts - timedelta(minutes=WATERMARK_MIN)
    ep = (wc - datetime(1970, 1, 1)).total_seconds()
    max_we = datetime.utcfromtimestamp(int(ep // SLIDE_SECONDS) * SLIDE_SECONDS)
    try:
        r = (
            spark.read.format("delta")
            .load(GOLD_AGGREGATED)
            .agg(F.max("window_end"))
            .first()
        )
        last_we = r[0]
        if last_we and hasattr(last_we, "astimezone"):
            last_we = last_we.astimezone(timezone.utc).replace(tzinfo=None)
    except:
        last_we = None
    if last_we:
        start_we = last_we + timedelta(seconds=SLIDE_SECONDS)
    else:
        min_ts = micro_df.select(
            F.least(
                F.min("request_datetime"),
                F.min("pickup_datetime"),
                F.min("dropoff_datetime"),
            ).alias("m")
        ).first()["m"]
        if hasattr(min_ts, "astimezone"):
            min_ts = min_ts.astimezone(timezone.utc).replace(tzinfo=None)
        ep2 = (min_ts - datetime(1970, 1, 1)).total_seconds()
        start_we = datetime.utcfromtimestamp(
            int(ep2 // SLIDE_SECONDS) * SLIDE_SECONDS + SLIDE_SECONDS
        )
    if start_we > max_we:
        return
    wes = []
    we = start_we
    while we <= max_we:
        wes.append(we)
        we += timedelta(seconds=SLIDE_SECONDS)
    log.info(f"Batch {batch_id}: {len(wes)} windows {wes[0]} → {wes[-1]}")
    for i in range(0, len(wes), 8):
        gb = build_gold_for_windows(spark, wes[i : i + 8])
        if gb and not gb.rdd.isEmpty():
            merge_into_gold(spark, gb)
            log.info(f"  Chunk {i//8+1} merged")


def main():
    spark = create_spark()
    stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER_COMPLETE)
        .select("request_datetime", "pickup_datetime", "dropoff_datetime")
    )
    # Trigger 15 giây = 1 window event-time (SPEED_FACTOR=60)
    (
        stream.writeStream.foreachBatch(lambda df, bid: process_batch(df, bid, spark))
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
