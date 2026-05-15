"""
ml/feature_builder.py
──────────────────────
Module dùng chung cho cả inference và training.

Thay đổi so với phiên bản cũ:
  1. ZONE_AREAS_KM2 — diện tích từng zone NYC TLC (263 zones), đơn vị km²
  2. compute_imbalance → chia thêm cho zone_area → chuẩn hóa mật độ
  3. build_inference_matrix_from_snapshots — inference nhanh từ 3 snapshot
     (không cần load 7 ngày liên tục, không dùng pandas shift)
  4. build_training_data — vẫn dùng full history + shift (unchanged)
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import date

# ── Zone areas (km²) — 263 NYC TLC zones ─────────────────────────────────────
# Nguồn: ước tính từ NYC TLC Taxi Zone shapefile.
# Các zone airport (JFK=132, LGA=138, EWR=1) có diện tích lớn nhất.
# Manhattan core zones rất nhỏ (0.1–0.6 km²).
# Cập nhật chính xác bằng: geopandas.read_file("taxi_zones.shp").to_crs(epsg=32618).area / 1e6
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

# ── Feature columns ───────────────────────────────────────────────────────────
LAG_MAIN_COLS = [
    "requests_60m",
    "requests_15m",
    "pickup_60m",
    "dropoff_60m",
    "pickup_delay_mean",
    "imbalance",
]
WEATHER_LEAD_STEPS = [1, 2, 3]  # T+15, T+30, T+45 phút
LAG_STEPS = [92, 668]  # ~23h, ~7 ngày

LAG_WEATHER_COLS = ["temperature_2m", "precipitation", "windspeed_10m", "weathercode"]

TEMPORAL_COLS = [
    "slot_sin",
    "slot_cos",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "woy_sin",
    "woy_cos",
    "is_weekend",
    "is_holiday",
    "month_num",
]

BASE_DEMAND_COLS = [
    "requests_60m",
    "requests_15m",
    "pickup_60m",
    "dropoff_60m",
    "pickup_delay_mean",
    "pickup_delay_std",
    "matched_rp",
    "matched_rd",
    "wav_requests",
    "aar_requests",
    "shared_requests",
    "uber_requests",
    "avg_fare",
    "avg_distance",
    "avg_driver_pay",
    "avg_tips",
    "neighbor_requests_60m",
    "neighbor_avg_requests_60m",
    "imbalance",
]

LAG_COLS = [f"{c}_lag{s}" for c in LAG_MAIN_COLS for s in LAG_STEPS]
WEATHER_LEAD_COLS = [
    f"{c}_lead{s}" for c in LAG_WEATHER_COLS for s in WEATHER_LEAD_STEPS
]

ALL_FEATURE_COLS = TEMPORAL_COLS + BASE_DEMAND_COLS + LAG_COLS + WEATHER_LEAD_COLS
NO_WEATHER_FEATURE_COLS = TEMPORAL_COLS + BASE_DEMAND_COLS + LAG_COLS  # fallback

import holidays

# US holidays (NYC dùng US là đủ)
US_HOLIDAYS = holidays.US(years=[2025, 2026])


# ── Core helpers ──────────────────────────────────────────────────────────────


def compute_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    """
    imbalance = (pickup_delay_mean^1.2 × requests_60m) / zone_area_km2

    Chia cho diện tích để chuẩn hóa: zone nhỏ (Manhattan) có mật độ cao hơn
    thực sự thay vì chỉ vì diện tích nhỏ.
    Fallback area = 1.0 km² nếu zone_id không có trong map.
    """
    df = df.copy()
    area = df["zone_id"].map(ZONE_AREAS_KM2).fillna(1.0)
    valid = df["pickup_delay_mean"].notna() & df["requests_60m"].notna()
    df["imbalance"] = np.where(
        valid,
        (df["pickup_delay_mean"].clip(lower=0) ** 1.2) * df["requests_60m"] / area,
        0.0,
    )
    return df


def _add_temporal(df: pd.DataFrame) -> pd.DataFrame:
    we = pd.to_datetime(df["window_end"])
    slot = (we.dt.hour * 4 + we.dt.minute // 15).astype(float)
    dow = we.dt.dayofweek.astype(float)
    woy = we.dt.isocalendar().week.astype(float)
    df["slot_sin"] = np.sin(2 * np.pi * slot / 96)
    df["slot_cos"] = np.cos(2 * np.pi * slot / 96)
    df["hour_sin"] = np.sin(2 * np.pi * we.dt.hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * we.dt.hour / 24)
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    df["woy_sin"] = np.sin(2 * np.pi * woy / 53)
    df["woy_cos"] = np.cos(2 * np.pi * woy / 53)
    df["is_weekend"] = (dow >= 5).astype(int)
    # ===== HOLIDAY WINDOW ±1 DAY =====
    holiday_dates = pd.to_datetime(list(US_HOLIDAYS.keys()))

    # tạo set gồm holiday, holiday-1, holiday+1
    holiday_window = pd.Index(
        np.concatenate(
            [
                holiday_dates,
                holiday_dates - pd.Timedelta(days=1),
                holiday_dates + pd.Timedelta(days=1),
            ]
        )
    )

    df["is_holiday"] = we.dt.normalize().isin(holiday_window).astype(int)
    df["month_num"] = we.dt.month.astype(float)
    return df


def _add_lag_features_shift(df: pd.DataFrame) -> pd.DataFrame:
    """Dùng cho training — cần series liên tục để shift() đúng."""
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
    """
    Dùng cho inference — merge trực tiếp từ snapshot tại lag step.
    lag_snapshots = {92: df_lag92, 668: df_lag668}
    """
    for col_name in LAG_MAIN_COLS:
        for step, snap in lag_snapshots.items():
            feat = f"{col_name}_lag{step}"
            if col_name in snap.columns:
                mapping = snap.set_index("zone_id")[col_name]
                df[feat] = df["zone_id"].map(mapping)
            else:
                df[feat] = np.nan
    return df


def inject_weather_leads(
    df: pd.DataFrame,
    weather_df: pd.DataFrame,
    zone_id_col: str = "zone_id",
    window_col: str = "window_end",
) -> pd.DataFrame:
    """Điền weather lead T+15, T+30, T+45 vào feature matrix."""
    df = df.copy()
    wdf = weather_df.copy()
    wdf["window_end"] = pd.to_datetime(wdf["window_end"])
    for step in WEATHER_LEAD_STEPS:
        lead_we = pd.to_datetime(df[window_col]) + pd.Timedelta(minutes=15 * step)
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

    # ── Training (full history) ───────────────────────────────────────────────
    @classmethod
    def build_training_data(
        cls,
        gold_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
        label_shift: int = 4,
        quantile_thresholds: list[float] | None = None,
    ) -> pd.DataFrame:
        """
        Input : gold/aggregated (nhiều tháng)
        Output: feature matrix với cột label_6class
        """
        df = gold_df.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = compute_imbalance(df)
        df = _add_temporal(df)
        df = _add_lag_features_shift(df)

        if weather_df is not None:
            df = inject_weather_leads(df, weather_df)
        else:
            for c in WEATHER_LEAD_COLS:
                df[c] = np.nan

        # Label: imbalance 4 slot tới (1 giờ)
        df = df.sort_values(["zone_id", "window_end"])
        df["future_imbalance"] = df.groupby("zone_id")["imbalance"].shift(-label_shift)
        df = df.dropna(subset=["future_imbalance"])

        # 6-class phân loại
        if quantile_thresholds is None:
            qs = df["future_imbalance"].quantile([0.2, 0.4, 0.6, 0.8, 0.95]).tolist()
        else:
            qs = quantile_thresholds
        df["label_6class"] = pd.cut(
            df["future_imbalance"],
            bins=[-np.inf] + qs + [np.inf],
            labels=[0, 1, 2, 3, 4, 5],
        ).astype(int)

        # Đảm bảo đủ feature cols
        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan

        return df, qs

    # ── Inference fast (3 snapshots) ─────────────────────────────────────────
    @classmethod
    def build_inference_matrix_from_snapshots(
        cls,
        current_df: pd.DataFrame,
        lag92_df: pd.DataFrame,
        lag668_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        Inference nhanh — không cần 7 ngày liên tục.
        Nhận 3 snapshot của gold/aggregated:
          current_df  : window_end hiện tại  (263 rows)
          lag92_df    : window_end - 92 slots ≈ 23h trước (263 rows)
          lag668_df   : window_end - 668 slots ≈ 7 ngày trước (263 rows)

        Trả về feature matrix 263 × ALL_FEATURE_COLS.
        """
        df = current_df.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = compute_imbalance(df)
        df = _add_temporal(df)

        # Tính imbalance cho các lag snapshots
        lag_snaps: dict[int, pd.DataFrame] = {}
        for step, snap in [(92, lag92_df), (668, lag668_df)]:
            s = snap.copy() if not snap.empty else pd.DataFrame(columns=["zone_id"])
            if not s.empty:
                s = compute_imbalance(s)
            lag_snaps[step] = s

        df = _add_lag_features_join(df, lag_snaps)

        if weather_df is not None:
            df = inject_weather_leads(df, weather_df)
        else:
            for c in WEATHER_LEAD_COLS:
                df[c] = np.nan

        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan

        return df[["zone_id", "window_end"] + ALL_FEATURE_COLS].copy()

    # ── Legacy: inference từ full history (vẫn giữ để backward compat) ────────
    @classmethod
    def build_inference_matrix(
        cls,
        history_df: pd.DataFrame,
        weather_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Giữ lại để tránh break code cũ. Prefer build_inference_matrix_from_snapshots."""
        df = history_df.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = compute_imbalance(df)
        df = _add_temporal(df)
        df = _add_lag_features_shift(df)
        if weather_df is not None:
            df = inject_weather_leads(df, weather_df)
        else:
            for c in WEATHER_LEAD_COLS:
                df[c] = np.nan
        for c in ALL_FEATURE_COLS:
            if c not in df.columns:
                df[c] = np.nan
        latest = df.groupby("zone_id")["window_end"].transform("max")
        return df[df["window_end"] == latest][
            ["zone_id", "window_end"] + ALL_FEATURE_COLS
        ].copy()
