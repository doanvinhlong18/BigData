"""
ml/feature_builder.py
──────────────────────
Module dùng chung inference + training.

Gold schema (input — sau khi join weather ở streaming):
  zone_id, window_end
  requests_60m, requests_15m, wav_req, aar_req, share_req, uber_req
  pickup_60m, pickup_15m, pickup_delay_mean, pickup_delay_std
  matched_rp, matched_rp_15m, wav_match, share_match
  dropoff_60m, dropoff_15m, matched_rd
  avg_trip_time, avg_fare, avg_driver_pay, avg_tips, avg_bcf,
  avg_tolls, avg_congestion_surcharge, avg_airport_fee,
  avg_sales_tax, avg_cbd_congestion_fee, avg_distance
  neighbor_requests_60m, neighbor_requests_15m,
  neighbor_pickup_delay_mean, num_neighbors
  temperature_2m, relative_humidity_2m, surface_pressure,
  precipitation, rain, snowfall, cloud_cover, weather_code,
  wind_speed_10m, wind_gusts_10m

Tính trong module này (không lưu trong Gold):
  imbalance = pickup_delay_mean^1.2 × requests_60m
  temporal: is_weekend, is_holiday, slot/dow/hou/woy/mon sin+cos

Lag features (tính từ Gold history, no shuffle):
  7 main cols × lag[92, 668]
  5 weather cols × lead[1,2,3] (notebook không có weather lags)
  lead1-3 weather: inject từ CSV (3 rows) vào trước khi predict

Quantile thresholds:
  Không hardcode, không lưu per-row — load từ MLflow params khi cần.
  DEFAULT_QUANTILES chỉ dùng lần đầu chưa có model.
"""

import math, logging
import numpy as np
import pandas as pd
from typing import Tuple, Optional, Dict

log = logging.getLogger("feature_builder")

DEFAULT_QUANTILES = {
    "Q1": 5_810.77,
    "Q2": 24_602.36,
    "Q3": 60_866.91,
    "Q4": 122_262.71,
    "Q5": 208_697.83,
}

US_HOLIDAYS = [
    "2026-01-01",
    "2026-01-15",
    "2026-02-19",
    "2026-05-27",
    "2026-06-19",
    "2026-07-04",
    "2026-09-02",
    "2026-10-14",
    "2026-11-11",
    "2026-11-28",
    "2026-12-25",
    "2025-01-01",
    "2025-01-20",
    "2025-02-17",
    "2025-05-26",
    "2025-06-19",
    "2025-07-04",
    "2025-09-01",
    "2025-10-13",
    "2025-11-11",
    "2025-11-27",
    "2025-12-25",
    "2024-01-01",
    "2024-01-15",
    "2024-02-19",
    "2024-05-27",
    "2024-06-19",
    "2024-07-04",
    "2024-09-02",
    "2024-10-14",
    "2024-11-11",
    "2024-11-28",
    "2024-12-25",
]

LAG_MAIN_COLS = [
    "requests_60m",
    "pickup_60m",
    "dropoff_60m",
    "pickup_delay_mean",
    "neighbor_requests_60m",
    "neighbor_pickup_delay_mean",
    "imbalance",
]
LAG_STEPS = [92, 668]

LAG_WEATHER_COLS = [
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "weather_code",
]
WEATHER_LAG_STEPS = []  # notebook không có weather lags — chỉ có leads
WEATHER_LEAD_STEPS = [1, 2, 3]

WEATHER_RAW_COLS = [
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

GOLD_RAW_FEATURES = [
    "requests_60m",
    "requests_15m",
    "wav_req",
    "aar_req",
    "share_req",
    "uber_req",
    "pickup_60m",
    "pickup_15m",
    "pickup_delay_mean",
    "pickup_delay_std",
    "matched_rp",
    "matched_rp_15m",
    "wav_match",
    "share_match",
    "dropoff_60m",
    "dropoff_15m",
    "matched_rd",
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
    "neighbor_requests_60m",
    "neighbor_requests_15m",
    "neighbor_pickup_delay_mean",
    "num_neighbors",
    "imbalance",
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

MODEL_FEATURE_SCHEMA_VERSION = "2.0"


def compute_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["imbalance"] = np.where(
        df["pickup_delay_mean"].notna() & df["requests_60m"].notna(),
        (df["pickup_delay_mean"].clip(lower=0) ** 1.2) * df["requests_60m"],
        0.0,
    )
    return df


def compute_temporal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    PI2 = 2 * math.pi
    we = pd.to_datetime(df["window_end"])
    df["is_weekend"] = we.dt.dayofweek.isin([5, 6]).astype(int)
    df["is_holiday"] = we.dt.strftime("%Y-%m-%d").isin(US_HOLIDAYS).astype(int)
    slot = we.dt.minute // 15
    dow = we.dt.dayofweek + 1
    hou = we.dt.hour
    woy = we.dt.isocalendar().week.astype(int)
    mon = we.dt.month
    df["slot_15m_sin"] = np.sin(PI2 * slot / 4)
    df["slot_15m_cos"] = np.cos(PI2 * slot / 4)
    df["dow_sin"] = np.sin(PI2 * dow / 7)
    df["dow_cos"] = np.cos(PI2 * dow / 7)
    df["hou_sin"] = np.sin(PI2 * hou / 24)
    df["hou_cos"] = np.cos(PI2 * hou / 24)
    df["woy_sin"] = np.sin(PI2 * woy / 52)
    df["woy_cos"] = np.cos(PI2 * woy / 52)
    df["mon_sin"] = np.sin(PI2 * mon / 12)
    df["mon_cos"] = np.cos(PI2 * mon / 12)
    return df


def assign_label_class(imbalance_val: float, quantiles: Dict) -> int:
    if pd.isna(imbalance_val):
        return np.nan
    q = quantiles
    if imbalance_val < q["Q1"]:
        return 0
    if imbalance_val < q["Q2"]:
        return 1
    if imbalance_val < q["Q3"]:
        return 2
    if imbalance_val < q["Q4"]:
        return 3
    if imbalance_val < q["Q5"]:
        return 4
    return 5


def _add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Tính lag/lead bằng pandas shift — no shuffle, rẻ."""
    df = df.sort_values(["zone_id", "window_end"]).reset_index(drop=True)
    for col in LAG_MAIN_COLS:
        if col not in df.columns:
            continue
        for step in LAG_STEPS:
            df[f"{col}_lag{step}"] = df.groupby("zone_id")[col].shift(step)
    for col in LAG_WEATHER_COLS:
        if col not in df.columns:
            continue
        for step in WEATHER_LAG_STEPS:
            df[f"{col}_lag{step}"] = df.groupby("zone_id")[col].shift(step)
        for step in WEATHER_LEAD_STEPS:
            # lead từ shift âm — nhưng Gold chưa có tương lai
            # → inject_weather_leads() sẽ override các cột này
            df[f"{col}_lead{step}"] = df.groupby("zone_id")[col].shift(-step)
    return df


def _build_all_feature_columns() -> list:
    cols = list(GOLD_RAW_FEATURES)
    for col in LAG_MAIN_COLS:
        for step in LAG_STEPS:
            cols.append(f"{col}_lag{step}")
    for col in LAG_WEATHER_COLS:
        for step in WEATHER_LAG_STEPS:
            cols.append(f"{col}_lag{step}")
        for step in WEATHER_LEAD_STEPS:
            cols.append(f"{col}_lead{step}")
    return cols


ALL_FEATURE_COLS = _build_all_feature_columns()

# Features cho Model B (fallback) — bỏ toàn bộ weather cols
_WEATHER_FEATURE_COLS_SET = set(
    WEATHER_RAW_COLS
    + [f"{c}_lag{s}" for c in LAG_WEATHER_COLS for s in WEATHER_LAG_STEPS]
    + [f"{c}_lead{s}" for c in LAG_WEATHER_COLS for s in WEATHER_LEAD_STEPS]
)
NO_WEATHER_FEATURE_COLS = [
    c for c in ALL_FEATURE_COLS if c not in _WEATHER_FEATURE_COLS_SET
]


class FeatureBuilder:

    @classmethod
    def build_inference_matrix(cls, gold_history_df: pd.DataFrame) -> pd.DataFrame:
        """
        Input:  7 ngày Gold history đã được merge weather từ CSV bởi caller
                (Gold không lưu weather — predict_dag merge trước khi gọi hàm này)
        Output: 263 rows × ALL_FEATURE_COLS

        Weather lags tính từ lịch sử trong df (sau khi caller merge weather vào).
        Weather leads (T+15/30/45) vẫn là NaN → gọi inject_weather_leads() sau.
        """
        df = gold_history_df.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = compute_imbalance(df)
        df = compute_temporal(df)
        df = _add_lag_features(df)
        latest = df.groupby("zone_id").last().reset_index()
        for col in ALL_FEATURE_COLS:
            if col not in latest.columns:
                latest[col] = np.nan
        return latest[["zone_id", "window_end"] + ALL_FEATURE_COLS].copy()

    @classmethod
    def inject_weather_leads(
        cls, X: pd.DataFrame, weather_leads_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Inject ONLY weather lead values (T+15, T+30, T+45).

        weather_leads_df: đọc từ silver/weather, chỉ 3 slots tương lai
                          columns: zone_id, window_end, LAG_WEATHER_COLS
        Lead 1 = window_end + 15 phút, lead 2 = +30, lead 3 = +45.
        """
        X = X.copy()
        we_vals = sorted(weather_leads_df["window_end"].unique())

        for step, future_ts in enumerate(we_vals[:3], start=1):
            rows = weather_leads_df[weather_leads_df["window_end"] == future_ts][
                ["zone_id"] + LAG_WEATHER_COLS
            ].copy()
            if rows.empty:
                log.warning(f"No weather for lead{step} at {future_ts}")
                continue
            rows = rows.rename(columns={c: f"{c}_lead{step}" for c in LAG_WEATHER_COLS})
            X = X.merge(rows, on="zone_id", how="left", suffixes=("", "_new"))
            for c in LAG_WEATHER_COLS:
                new_c = f"{c}_lead{step}_new"
                tgt_c = f"{c}_lead{step}"
                if new_c in X.columns:
                    X[tgt_c] = X[new_c].combine_first(
                        X.get(tgt_c, pd.Series(dtype=float))
                    )
                    X = X.drop(columns=[new_c])
        return X

    @classmethod
    def build_training_data(
        cls, gold_df: pd.DataFrame, quantiles: Optional[Dict] = None
    ) -> Tuple[pd.DataFrame, pd.Series, Dict]:
        """
        Input:  12 tháng Gold (đã có weather từ streaming join)
        Output: X, y, quantiles_used

        quantiles=None → tự tính từ data → caller lưu vào MLflow.
        """
        df = gold_df.copy()
        df["window_end"] = pd.to_datetime(df["window_end"])
        df = compute_imbalance(df)
        df = compute_temporal(df)
        df = df.sort_values(["zone_id", "window_end"]).reset_index(drop=True)
        df = _add_lag_features(df)

        df["label_raw"] = df.groupby("zone_id")["imbalance"].shift(-4)

        if quantiles is None:
            vals = df["label_raw"].dropna()
            qs = np.quantile(vals, [0.10, 0.25, 0.50, 0.75, 0.90])
            quantiles = {
                "Q1": float(qs[0]),
                "Q2": float(qs[1]),
                "Q3": float(qs[2]),
                "Q4": float(qs[3]),
                "Q5": float(qs[4]),
            }
            log.info(f"Computed quantiles: {quantiles}")
            for k, v in DEFAULT_QUANTILES.items():
                new_v = quantiles[k]
                if v > 0 and abs(new_v - v) / v > 0.20:
                    log.warning(
                        f"Quantile {k} drifted: {v:.1f} → {new_v:.1f} ({(new_v-v)/v*100:.1f}%)"
                    )

        df["label_6class"] = df["label_raw"].apply(
            lambda x: assign_label_class(x, quantiles) if pd.notna(x) else np.nan
        )
        df = df.dropna(subset=["label_6class"])
        df["label_6class"] = df["label_6class"].astype(int)

        for col in ALL_FEATURE_COLS:
            if col not in df.columns:
                df[col] = np.nan

        X = df[ALL_FEATURE_COLS].copy()
        y = df["label_6class"]
        log.info(
            f"Training: {len(X):,} rows × {len(X.columns)} features | "
            f"label dist:\n{y.value_counts().sort_index()}"
        )
        return X, y, quantiles

    @classmethod
    def get_feature_columns(cls) -> list:
        """Tất cả features — dùng cho Model A (có weather)."""
        return list(ALL_FEATURE_COLS)

    @classmethod
    def get_no_weather_feature_columns(cls) -> list:
        """Features không có weather — dùng cho Model B (fallback)."""
        return list(NO_WEATHER_FEATURE_COLS)

    @classmethod
    def get_schema_version(cls) -> str:
        return MODEL_FEATURE_SCHEMA_VERSION
