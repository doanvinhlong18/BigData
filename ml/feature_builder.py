"""
ml/feature_builder.py
──────────────────────
ALL_FEATURE_COLS khớp chính xác với m.feature_name() từ lgb_final_model.txt
(verified, không cần đoán từ notebook nữa).
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import date

# ── Zone areas (km²) ─────────────────────────────────────────────────────────
ZONE_AREAS_KM2: dict[int, float] = {
    1: 29.8,
    2: 4.8,
    3: 1.2,
    4: 0.7,
    5: 3.2,
    6: 5.1,
    7: 0.6,
    8: 0.8,
    9: 1.5,
    10: 1.9,
    11: 1.3,
    12: 0.5,
    13: 0.6,
    14: 1.1,
    15: 2.4,
    16: 1.8,
    17: 2.3,
    18: 2.1,
    19: 1.4,
    20: 3.2,
    21: 1.6,
    22: 1.7,
    23: 2.8,
    24: 0.4,
    25: 1.3,
    26: 1.5,
    27: 2.6,
    28: 1.9,
    29: 1.8,
    30: 0.7,
    31: 3.1,
    32: 2.4,
    33: 1.2,
    34: 1.5,
    35: 2.3,
    36: 2.8,
    37: 2.4,
    38: 1.6,
    39: 2.2,
    40: 1.8,
    41: 0.3,
    42: 0.4,
    43: 0.5,
    44: 2.1,
    45: 0.3,
    46: 15.2,
    47: 1.4,
    48: 0.6,
    49: 2.8,
    50: 0.7,
    51: 2.1,
    52: 2.4,
    53: 1.8,
    54: 0.9,
    55: 2.7,
    56: 2.3,
    57: 4.2,
    58: 2.8,
    59: 1.3,
    60: 1.4,
    61: 1.6,
    62: 1.9,
    63: 1.1,
    64: 1.7,
    65: 1.5,
    66: 1.8,
    67: 0.6,
    68: 0.4,
    69: 0.5,
    70: 1.3,
    71: 2.1,
    72: 1.8,
    73: 2.4,
    74: 0.3,
    75: 0.4,
    76: 2.3,
    77: 2.1,
    78: 1.6,
    79: 0.5,
    80: 3.2,
    81: 1.8,
    82: 3.4,
    83: 2.6,
    84: 1.4,
    85: 3.1,
    86: 2.8,
    87: 0.5,
    88: 0.4,
    89: 3.6,
    90: 2.2,
    91: 2.9,
    92: 1.7,
    93: 2.3,
    94: 2.1,
    95: 2.4,
    96: 1.8,
    97: 1.3,
    98: 2.7,
    99: 2.2,
    100: 0.3,
    101: 1.9,
    102: 2.4,
    103: 11.2,
    104: 3.8,
    105: 4.2,
    106: 1.6,
    107: 1.4,
    108: 1.7,
    109: 2.3,
    110: 3.1,
    111: 1.9,
    112: 2.4,
    113: 0.4,
    114: 0.5,
    115: 5.3,
    116: 0.3,
    117: 2.8,
    118: 2.9,
    119: 1.4,
    120: 0.4,
    121: 2.6,
    122: 1.3,
    123: 0.6,
    124: 3.7,
    125: 0.5,
    126: 0.6,
    127: 0.4,
    128: 0.4,
    129: 2.4,
    130: 3.2,
    131: 1.6,
    132: 18.5,
    133: 1.8,
    134: 2.1,
    135: 1.9,
    136: 1.7,
    137: 1.6,
    138: 7.2,
    139: 2.3,
    140: 0.6,
    141: 0.7,
    142: 1.8,
    143: 2.3,
    144: 2.8,
    145: 1.4,
    146: 1.6,
    147: 0.5,
    148: 0.6,
    149: 0.5,
    150: 1.8,
    151: 0.4,
    152: 1.2,
    153: 2.4,
    154: 8.7,
    155: 2.1,
    156: 0.5,
    157: 1.4,
    158: 0.6,
    159: 1.3,
    160: 1.6,
    161: 0.3,
    162: 0.4,
    163: 2.8,
    164: 1.2,
    165: 3.1,
    166: 3.8,
    167: 2.1,
    168: 1.4,
    169: 1.8,
    170: 2.4,
    171: 3.2,
    172: 6.4,
    173: 2.8,
    174: 1.6,
    175: 2.3,
    176: 1.9,
    177: 4.8,
    178: 2.1,
    179: 0.4,
    180: 3.2,
    181: 1.8,
    182: 2.4,
    183: 3.6,
    184: 1.9,
    185: 0.4,
    186: 2.6,
    187: 1.8,
    188: 3.2,
    189: 0.6,
    190: 1.4,
    191: 1.6,
    192: 2.3,
    193: 0.7,
    194: 0.3,
    195: 3.8,
    196: 2.4,
    197: 3.1,
    198: 1.6,
    199: 8.4,
    200: 2.8,
    201: 2.1,
    202: 0.6,
    203: 3.2,
    204: 4.6,
    205: 2.8,
    206: 2.1,
    207: 1.4,
    208: 6.8,
    209: 1.8,
    210: 1.6,
    211: 0.5,
    212: 3.4,
    213: 9.2,
    214: 2.8,
    215: 1.6,
    216: 3.2,
    217: 2.1,
    218: 1.8,
    219: 0.8,
    220: 2.4,
    221: 0.6,
    222: 7.8,
    223: 0.7,
    224: 0.4,
    225: 3.6,
    226: 2.1,
    227: 0.5,
    228: 0.6,
    229: 0.4,
    230: 0.5,
    231: 1.8,
    232: 2.4,
    233: 0.6,
    234: 2.8,
    235: 4.2,
    236: 1.6,
    237: 0.5,
    238: 1.8,
    239: 2.3,
    240: 2.1,
    241: 3.8,
    242: 11.4,
    243: 0.4,
    244: 3.2,
    245: 2.1,
    246: 0.3,
    247: 0.4,
    248: 2.8,
    249: 0.6,
    250: 5.4,
    251: 3.8,
    252: 1.6,
    253: 2.4,
    254: 3.1,
    255: 4.2,
    256: 6.8,
    257: 5.2,
    258: 4.6,
    259: 7.3,
    260: 2.8,
    261: 0.5,
    262: 0.4,
    263: 1.2,
}

# ── Zone neighbors (adjacency map) ───────────────────────────────────────────
# Single source of truth — đã bỏ khỏi silver_to_gold.py (v4), tính tại predict
ZONE_NEIGHBORS: dict[int, list[int]] = {
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
    256: [],
}

# ── Internal constants ────────────────────────────────────────────────────────
LAG_MAIN_COLS = [
    "requests_60m",
    "pickup_60m",
    "dropoff_60m",
    "pickup_delay_mean",
    "neighbor_requests_60m",
    "neighbor_pickup_delay_mean",
    "imbalance",
]
LAG_WEATHER_COLS = [
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "weather_code",
]
WEATHER_LEAD_STEPS = [1, 2, 3]
LAG_STEPS = [92, 668]

# ── ALL_FEATURE_COLS — verified từ m.feature_name() ──────────────────────────
ALL_FEATURE_COLS: list[str] = [
    "zone_id",
    # demand
    "requests_60m",
    "wav_req",
    "aar_req",
    "share_req",
    "uber_req",
    # pickup
    "pickup_60m",
    "pickup_delay_mean",
    "pickup_delay_std",
    "matched_rp",
    "wav_match",
    "share_match",
    # dropoff
    "dropoff_60m",
    "matched_rd",
    # dropoff stats
    "avg_trip_time",
    "avg_fare",
    "avg_driver_pay",
    "avg_tips",
    "avg_bcf",
    "avg_tolls",
    "avg_congestion_surcharge",
    "avg_airport_fee",
    "avg_sales_tax",
    "avg_cbd_congestion_fee",
    "avg_distance",
    # weather raw
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
    # neighbor
    "neighbor_requests_60m",
    "neighbor_pickup_delay_mean",
    "num_neighbors",
    # imbalance
    "imbalance",
    # temporal (cyclical only — scalar cols bị drop trước khi ghi parquet)
    "is_weekend",
    "is_holiday",
    "slot_15m_sin",
    "slot_15m_cos",
    "dow_sin",
    "dow_cos",
    "hou_sin",
    "hou_cos",
    "woy_sin",
    "woy_cos",
    "mon_sin",
    "mon_cos",
    # lags
    "requests_60m_lag92",
    "requests_60m_lag668",
    "pickup_60m_lag92",
    "pickup_60m_lag668",
    "dropoff_60m_lag92",
    "dropoff_60m_lag668",
    "pickup_delay_mean_lag92",
    "pickup_delay_mean_lag668",
    "neighbor_requests_60m_lag92",
    "neighbor_requests_60m_lag668",
    "neighbor_pickup_delay_mean_lag92",
    "neighbor_pickup_delay_mean_lag668",
    "imbalance_lag92",
    "imbalance_lag668",
    # weather leads
    "temperature_2m_lead1",
    "temperature_2m_lead2",
    "temperature_2m_lead3",
    "relative_humidity_2m_lead1",
    "relative_humidity_2m_lead2",
    "relative_humidity_2m_lead3",
    "surface_pressure_lead1",
    "surface_pressure_lead2",
    "surface_pressure_lead3",
    "cloud_cover_lead1",
    "cloud_cover_lead2",
    "cloud_cover_lead3",
    "weather_code_lead1",
    "weather_code_lead2",
    "weather_code_lead3",
]

# ── NO_WEATHER_FEATURE_COLS — Model B (bỏ raw weather + tất cả leads) ────────
_WEATHER_COLS = {
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
    "temperature_2m_lead1",
    "temperature_2m_lead2",
    "temperature_2m_lead3",
    "relative_humidity_2m_lead1",
    "relative_humidity_2m_lead2",
    "relative_humidity_2m_lead3",
    "surface_pressure_lead1",
    "surface_pressure_lead2",
    "surface_pressure_lead3",
    "cloud_cover_lead1",
    "cloud_cover_lead2",
    "cloud_cover_lead3",
    "weather_code_lead1",
    "weather_code_lead2",
    "weather_code_lead3",
}
NO_WEATHER_FEATURE_COLS: list[str] = [
    c for c in ALL_FEATURE_COLS if c not in _WEATHER_COLS
]

import holidays

US_HOLIDAYS = holidays.US(years=list(range(2024, 2027)))


# ── Core helpers ──────────────────────────────────────────────────────────────


def _normalize_datetime_utc(values):
    converted = pd.to_datetime(values, utc=True)
    if isinstance(converted, pd.Series):
        return converted.dt.floor("us")
    if isinstance(converted, pd.DatetimeIndex):
        return converted.floor("us")
    return pd.Timestamp(converted).floor("us")


def _inference_output_cols() -> list[str]:
    return ["zone_id", "window_end"] + [
        c for c in ALL_FEATURE_COLS if c != "zone_id"
    ]


def compute_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    area = df["zone_id"].map(ZONE_AREAS_KM2).fillna(1.0)
    valid = df["pickup_delay_mean"].notna() & df["requests_60m"].notna()
    df["imbalance"] = np.where(
        valid,
        (df["pickup_delay_mean"].clip(lower=0) ** 1.2) * df["requests_60m"] / area,
        0.0,
    )
    return df


def _add_neighbor_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính neighbor features từ ZONE_NEIGHBORS dict tĩnh.
    Đồng bộ notebook cell 21 compute_neighbor_requests().

    Features thêm vào:
      neighbor_requests_60m      : tổng requests_60m của các zone lân cận
      neighbor_pickup_delay_mean : mean pickup_delay_mean của các zone lân cận
      num_neighbors              : số neighbor có data trong window

    Đã bỏ khỏi silver_to_gold.py (v4) — tính ở đây bằng pandas nhanh hơn Spark join.
    """
    base = df[["zone_id", "requests_60m", "pickup_delay_mean"]].copy()
    base = base.rename(
        columns={
            "zone_id": "neighbor_zone_id",
            "requests_60m": "n_requests_60m",
            "pickup_delay_mean": "n_pickup_delay_mean",
        }
    )

    # Explode adjacency map thành DataFrame
    adj = pd.DataFrame(
        [
            {"zone_id": z, "neighbor_zone_id": n}
            for z, neighbors in ZONE_NEIGHBORS.items()
            for n in neighbors
        ]
    )

    if adj.empty:
        df["neighbor_requests_60m"] = 0.0
        df["neighbor_pickup_delay_mean"] = 0.0
        df["num_neighbors"] = 0
        return df

    # Join adj → neighbor data
    adj = adj.merge(base, on="neighbor_zone_id", how="left")

    agg = (
        adj.groupby("zone_id")
        .agg(
            neighbor_requests_60m=("n_requests_60m", "sum"),
            neighbor_pickup_delay_mean=("n_pickup_delay_mean", "mean"),
            num_neighbors=("neighbor_zone_id", "count"),
        )
        .reset_index()
    )

    df = df.merge(agg, on="zone_id", how="left")
    df["neighbor_requests_60m"] = df["neighbor_requests_60m"].fillna(0.0)
    df["neighbor_pickup_delay_mean"] = df["neighbor_pickup_delay_mean"].fillna(0.0)
    df["num_neighbors"] = df["num_neighbors"].fillna(0).astype(int)
    return df


def _add_temporal(df: pd.DataFrame) -> pd.DataFrame:
    we = _normalize_datetime_utc(df["window_end"])

    slot_15m = we.dt.minute // 15
    df["slot_15m_sin"] = np.sin(2 * np.pi * slot_15m / 4)
    df["slot_15m_cos"] = np.cos(2 * np.pi * slot_15m / 4)

    df["hou_sin"] = np.sin(2 * np.pi * we.dt.hour / 24)
    df["hou_cos"] = np.cos(2 * np.pi * we.dt.hour / 24)

    dow = we.dt.dayofweek.astype(float)
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)

    woy = we.dt.isocalendar().week.astype(float)
    df["woy_sin"] = np.sin(2 * np.pi * woy / 52)
    df["woy_cos"] = np.cos(2 * np.pi * woy / 52)

    month = we.dt.month.astype(float)
    df["mon_sin"] = np.sin(2 * np.pi * month / 12)
    df["mon_cos"] = np.cos(2 * np.pi * month / 12)

    df["is_weekend"] = (dow >= 5).astype(int)

    holiday_dates = pd.to_datetime(list(US_HOLIDAYS.keys()))
    holiday_window_dates = set(
        pd.DatetimeIndex(
            np.concatenate(
                [
                    holiday_dates,
                    holiday_dates - pd.Timedelta(days=1),
                    holiday_dates + pd.Timedelta(days=1),
                ]
            )
        )
        .normalize()
        .unique()
        .date
    )
    df["is_holiday"] = we.dt.date.isin(holiday_window_dates).astype(int)
    return df


def _add_lag_features_shift(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["zone_id", "window_end"])
    for col in LAG_MAIN_COLS:
        if col not in df.columns:
            for s in LAG_STEPS:
                df[f"{col}_lag{s}"] = np.nan
            continue
        grp = df.groupby("zone_id")[col]
        for s in LAG_STEPS:
            df[f"{col}_lag{s}"] = grp.shift(s)
    return df


def _add_lag_features_join(
    df: pd.DataFrame,
    lag_snapshots: dict[int, pd.DataFrame],
) -> pd.DataFrame:
    for col_name in LAG_MAIN_COLS:
        for step, snap in lag_snapshots.items():
            feat = f"{col_name}_lag{step}"
            if col_name in snap.columns:
                df[feat] = df["zone_id"].map(snap.set_index("zone_id")[col_name])
            else:
                df[feat] = np.nan
    return df


def inject_weather_raw(
    df: pd.DataFrame,
    weather_df: pd.DataFrame,
    zone_id_col: str = "zone_id",
    window_col: str = "window_end",
) -> pd.DataFrame:
    """
    Inject weather values tại window_end HIỆN TẠI (step=0) vào df.

    Đây là các raw weather features mà model_a cần:
      temperature_2m, relative_humidity_2m, surface_pressure,
      precipitation, rain, snowfall, cloud_cover, weather_code,
      wind_speed_10m, wind_gusts_10m

    Gold/aggregated không lưu weather (silver_to_gold.py TODO) nên
    current_df không có các cột này. Hàm này merge từ weather_df theo
    (zone_id, window_end) để điền đúng giá trị thay vì để NaN.

    Gọi TRƯỚC inject_weather_leads() trong build_inference_matrix_from_snapshots()
    và build_inference_matrix().
    """
    # Tất cả weather columns (raw + cols có thể tồn tại trong weather_df)
    RAW_WEATHER_COLS = [
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
    ]

    df = df.copy()
    wdf = weather_df.copy()
    wdf["window_end"] = _normalize_datetime_utc(wdf["window_end"])
    df[window_col] = _normalize_datetime_utc(df[window_col])

    # Chỉ lấy các cột thực sự có trong weather_df
    available = [c for c in RAW_WEATHER_COLS if c in wdf.columns]
    if not available:
        return df

    wdf_slim = wdf[[zone_id_col, "window_end"] + available].drop_duplicates(
        subset=[zone_id_col, "window_end"]
    )

    # Reset index để đảm bảo alignment sau merge
    df_idx = df.reset_index(drop=True)
    merged = df_idx[[zone_id_col, window_col]].merge(
        wdf_slim.rename(columns={"window_end": window_col}),
        on=[zone_id_col, window_col],
        how="left",
    )

    # Điền vào df: ưu tiên giá trị từ weather_df, giữ nguyên nếu đã có
    for c in available:
        if c in df.columns:
            # Cột đã có → chỉ fill NaN (pandas .where giữ nguyên giá trị notna)
            df = df.reset_index(drop=True)
            df[c] = df[c].where(df[c].notna(), merged[c].values)
        else:
            df = df.reset_index(drop=True)
            df[c] = merged[c].values

    return df


def inject_weather_leads(
    df: pd.DataFrame,
    weather_df: pd.DataFrame,
    zone_id_col: str = "zone_id",
    window_col: str = "window_end",
) -> pd.DataFrame:
    """
    Inject weather values tại các bước TƯƠNG LAI (lead1, lead2, lead3).

    Chỉ thêm {wc}_lead1/2/3 — KHÔNG inject raw weather tại window_end hiện tại.
    Dùng inject_weather_raw() cho raw weather trước khi gọi hàm này.
    """
    df = df.copy()
    wdf = weather_df.copy()
    wdf["window_end"] = _normalize_datetime_utc(wdf["window_end"])
    df[window_col] = _normalize_datetime_utc(df[window_col])
    for step in WEATHER_LEAD_STEPS:
        lead_we = df[window_col] + pd.Timedelta(minutes=15 * step)
        tmp = df[[zone_id_col]].copy()
        tmp["_lead_we"] = lead_we
        tmp = tmp.merge(
            wdf.rename(columns={"window_end": "_lead_we"}),
            on=[zone_id_col, "_lead_we"],
            how="left",
        )
        for wc in LAG_WEATHER_COLS:
            df[f"{wc}_lead{step}"] = tmp[wc].values if wc in tmp.columns else np.nan
    return df


class FeatureBuilder:

    @classmethod
    def build_training_data(
        cls,
        gold_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
        label_shift: int = 4,
        quantile_thresholds: list[float] | None = None,
    ) -> pd.DataFrame:
        df = gold_df.copy()
        df["window_end"] = _normalize_datetime_utc(df["window_end"])
        df = compute_imbalance(df)
        df = _add_neighbor_features(df)
        df = _add_temporal(df)
        df = _add_lag_features_shift(df)

        if weather_df is not None:
            df = inject_weather_leads(df, weather_df)
        else:
            for wc in LAG_WEATHER_COLS:
                for s in WEATHER_LEAD_STEPS:
                    df[f"{wc}_lead{s}"] = np.nan

        df = df.sort_values(["zone_id", "window_end"])
        df["future_imbalance"] = df.groupby("zone_id")["imbalance"].shift(-label_shift)
        df = df.dropna(subset=["future_imbalance"])

        qs = (
            quantile_thresholds
            or df["future_imbalance"].quantile([0.2, 0.4, 0.6, 0.8, 0.95]).tolist()
        )
        df["label_6class"] = pd.cut(
            df["future_imbalance"],
            bins=[-np.inf] + qs + [np.inf],
            labels=[0, 1, 2, 3, 4, 5],
        ).astype(int)

        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan

        return df, qs

    @classmethod
    def build_inference_matrix_from_snapshots(
        cls,
        current_df: pd.DataFrame,
        lag92_df: pd.DataFrame,
        lag668_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        df = current_df.copy()
        df["window_end"] = _normalize_datetime_utc(df["window_end"])
        df = compute_imbalance(df)
        df = _add_neighbor_features(df)
        df = _add_temporal(df)

        lag_snaps: dict[int, pd.DataFrame] = {}
        for step, snap in [(92, lag92_df), (668, lag668_df)]:
            s = snap.copy() if not snap.empty else pd.DataFrame(columns=["zone_id"])
            if not s.empty:
                s["window_end"] = _normalize_datetime_utc(s["window_end"])
                s = compute_imbalance(s)
                s = _add_neighbor_features(s)  # lag cần neighbor cols của snapshot cũ
            lag_snaps[step] = s

        df = _add_lag_features_join(df, lag_snaps)

        if weather_df is not None:
            # FIX: inject raw weather tại window_end HIỆN TẠI trước
            # Gold/aggregated không lưu weather cols (silver_to_gold TODO),
            # nên phải merge từ weather_df để tránh NaN cho model_a.
            df = inject_weather_raw(df, weather_df)
            df = inject_weather_leads(df, weather_df)
        else:
            for wc in LAG_WEATHER_COLS:
                for s in WEATHER_LEAD_STEPS:
                    df[f"{wc}_lead{s}"] = np.nan

        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan

        return df[_inference_output_cols()].copy()

    @classmethod
    def build_inference_matrix(
        cls,
        history_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Giữ lại để tránh break code cũ."""
        df = history_df.copy()
        df["window_end"] = _normalize_datetime_utc(df["window_end"])
        df = compute_imbalance(df)
        df = _add_neighbor_features(df)
        df = _add_temporal(df)
        df = _add_lag_features_shift(df)
        if weather_df is not None:
            # FIX: inject raw weather tại window_end HIỆN TẠI trước
            df = inject_weather_raw(df, weather_df)
            df = inject_weather_leads(df, weather_df)
        else:
            for wc in LAG_WEATHER_COLS:
                for s in WEATHER_LEAD_STEPS:
                    df[f"{wc}_lead{s}"] = np.nan
        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan
        latest = df.groupby("zone_id")["window_end"].transform("max")
        return df[df["window_end"] == latest][_inference_output_cols()].copy()
