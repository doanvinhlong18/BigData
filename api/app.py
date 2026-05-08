"""
api/app.py
──────────
FastAPI — đọc Delta tables từ MinIO, expose REST endpoints cho Grafana Infinity plugin.

Endpoints:
  GET /health
  GET /stats/overall                      → 4 big numbers cho stat panels
  GET /demand/latest                      → 263 rows window_end mới nhất
  GET /demand/history?hours=24            → time series aggregate hoặc per zone
  GET /demand/zone?zone_id=132&hours=48   → time series 1 zone cụ thể
  GET /predictions/latest                 → predictions window mới nhất
  GET /predictions/history?hours=24       → accuracy trend qua thời gian
  GET /predictions/model_split            → tỉ lệ Model A vs B
  GET /predictions/per_zone?hours=24      → accuracy per zone
  GET /predictions/shadow_comparison      → so sánh Production vs Shadow
  GET /predictions/class_distribution     → phân phối predicted class

SCHEMA gold/predictions_monitoring (bảng gộp):
  zone_id, window_end
  predicted_class (0-5), proba_0..5, pred_confidence
  used_model ("model_a" | "model_b_fallback")
  model_version, feature_schema_version, predicted_at
  shadow_predicted_class (nullable), shadow_proba_0..5 (nullable)
  actual_imbalance (nullable), actual_label_class (nullable)
  is_correct (nullable), shadow_is_correct (nullable)
  evaluated_at (nullable)

PERFORMANCE NOTE:
  Mỗi request đọc Delta table → nếu Grafana refresh 5s sẽ nặng.
  Set Grafana panel refresh tối thiểu 1 phút.
  Endpoint /demand/latest và /predictions/latest có cache 60s.
"""

import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="BigData Metrics API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")

GOLD_AGG_PATH = "s3://gold/aggregated"
GOLD_PRED_PATH = "s3://gold/predictions_monitoring"

STORAGE_OPTS = {
    "aws_access_key_id": MINIO_ACCESS,
    "aws_secret_access_key": MINIO_SECRET,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "region_name": "us-east-1",
}

CLASS_LABELS = {
    0: "very low",
    1: "low",
    2: "medium",
    3: "high",
    4: "very high",
    5: "surge",
}


# ── Delta reader ──────────────────────────────────────────────────────────────


def load_delta(path: str, filters: list = None):
    from deltalake import DeltaTable

    dt = DeltaTable(path, storage_options=STORAGE_OPTS)
    return dt.to_pandas(filters=filters) if filters else dt.to_pandas()


def ts_filters(col: str, hours: int):
    """Tạo filter cho N giờ gần nhất."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    return [(col, ">=", since)]


def get_latest_window_end() -> str:
    """Window_end của slot 15-phút vừa đóng."""
    import pandas as pd

    return str(pd.Timestamp.utcnow().floor("15min") - pd.Timedelta(minutes=15))


def safe_ts(df, col: str):
    """Convert timestamp column sang string an toàn."""
    if col in df.columns:
        df[col] = df[col].astype(str)
    return df


# ── HEALTH ────────────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ── OVERALL STATS ─────────────────────────────────────────────────────────────


@app.get("/stats/overall")
def stats_overall():
    """
    4 số lớn cho Grafana stat panels.
    Đọc cả gold/aggregated và gold/predictions_monitoring.
    """
    try:
        now = datetime.now(timezone.utc)

        # Demand 24h
        df_agg = load_delta(GOLD_AGG_PATH, ts_filters("window_end", 24))

        # Predictions + actuals 24h
        df_pred = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", 24))
        evaluated = (
            df_pred[df_pred["evaluated_at"].notna()] if not df_pred.empty else df_pred
        )

        total_trips = int(df_agg["requests_60m"].sum()) if not df_agg.empty else 0
        n_windows = int(df_agg["window_end"].nunique()) if not df_agg.empty else 0
        avg_accuracy = (
            float(evaluated["is_correct"].mean()) if not evaluated.empty else None
        )
        n_evaluated = (
            int(evaluated["is_correct"].notna().sum()) if not evaluated.empty else 0
        )

        # Fallback rate (zone dùng model_b / tổng)
        fallback_rate = (
            (df_pred["used_model"] == "model_b_fallback").mean()
            if not df_pred.empty
            else None
        )

        # Shadow accuracy nếu có
        shadow_acc = None
        if not evaluated.empty and "shadow_is_correct" in evaluated.columns:
            shadow_vals = evaluated["shadow_is_correct"].dropna()
            if len(shadow_vals) > 0:
                shadow_acc = float(shadow_vals.mean())

        return {
            "total_requests_24h": total_trips,
            "windows_processed": n_windows,
            "avg_accuracy_24h": (
                round(avg_accuracy, 4) if avg_accuracy is not None else None
            ),
            "n_evaluated": n_evaluated,
            "fallback_rate": (
                round(float(fallback_rate), 4) if fallback_rate is not None else None
            ),
            "shadow_accuracy": round(shadow_acc, 4) if shadow_acc is not None else None,
            "last_updated": now.isoformat(),
        }
    except Exception as e:
        return {"error": str(e)}


# ── DEMAND ────────────────────────────────────────────────────────────────────


@app.get("/demand/latest")
def demand_latest():
    """
    263 rows của window_end mới nhất.
    Cột chính: requests_60m, requests_15m, pickup_delay_mean,
               imbalance, avg_fare, avg_distance, các neighbor features.
    """
    try:
        latest_we = get_latest_window_end()
        df = load_delta(GOLD_AGG_PATH, [("window_end", "=", latest_we)])

        if df.empty:
            # Thử window trước
            import pandas as pd

            prev_we = str(pd.Timestamp(latest_we) - pd.Timedelta(minutes=15))
            df = load_delta(GOLD_AGG_PATH, [("window_end", "=", prev_we)])

        if df.empty:
            return {"data": [], "window_end": None, "n_zones": 0}

        # Chỉ lấy columns Grafana cần — tránh gửi ~107 cols qua HTTP
        KEEP = [
            "zone_id",
            "window_end",
            "requests_60m",
            "requests_15m",
            "pickup_60m",
            "pickup_15m",
            "pickup_delay_mean",
            "pickup_delay_std",
            "dropoff_60m",
            "matched_rp",
            "imbalance",
            "avg_fare",
            "avg_driver_pay",
            "avg_trip_time",
            "avg_distance",
            "neighbor_requests_60m",
            "neighbor_pickup_delay_mean",
            "num_neighbors",
            "temperature_2m",
            "weather_code",
            "is_weekend",
            "is_holiday",
        ]
        cols = [c for c in KEEP if c in df.columns]
        df = safe_ts(df[cols], "window_end")

        return {
            "data": df.fillna(0).to_dict(orient="records"),
            "window_end": latest_we,
            "n_zones": len(df),
        }
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/demand/history")
def demand_history(
    hours: int = Query(default=24, ge=1, le=168),
):
    """
    Aggregate tất cả zones theo window_end — cho time series chart.
    Sum requests, mean pickup_delay, sum imbalance.
    """
    try:
        df = load_delta(GOLD_AGG_PATH, ts_filters("window_end", hours))
        if df.empty:
            return {"data": [], "hours": hours}

        df["window_end"] = df["window_end"].astype(str)
        agg = (
            df.groupby("window_end")
            .agg(
                total_requests_60m=("requests_60m", "sum"),
                total_requests_15m=("requests_15m", "sum"),
                total_pickup_60m=("pickup_60m", "sum"),
                avg_pickup_delay=("pickup_delay_mean", "mean"),
                total_imbalance=("imbalance", "sum"),
                avg_fare=("avg_fare", "mean"),
                n_zones=("zone_id", "count"),
            )
            .reset_index()
            .sort_values("window_end")
        )
        return {"data": agg.fillna(0).to_dict(orient="records"), "hours": hours}
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/demand/zone")
def demand_zone(
    zone_id: int = Query(..., ge=1, le=263),
    hours: int = Query(default=48, ge=1, le=168),
):
    """
    Time series 1 zone cụ thể — cho drill-down chart.
    """
    try:
        df = load_delta(
            GOLD_AGG_PATH,
            ts_filters("window_end", hours) + [("zone_id", "=", zone_id)],
        )
        if df.empty:
            return {"data": [], "zone_id": zone_id}

        KEEP = [
            "window_end",
            "requests_60m",
            "requests_15m",
            "pickup_delay_mean",
            "pickup_15m",
            "dropoff_60m",
            "imbalance",
            "avg_fare",
            "avg_distance",
            "neighbor_requests_60m",
            "temperature_2m",
            "weather_code",
        ]
        cols = [c for c in KEEP if c in df.columns]
        df = safe_ts(df[cols].sort_values("window_end"), "window_end")
        return {"data": df.fillna(0).to_dict(orient="records"), "zone_id": zone_id}
    except Exception as e:
        return {"error": str(e), "data": []}


# ── PREDICTIONS + MONITORING (bảng gộp) ──────────────────────────────────────


@app.get("/predictions/latest")
def predictions_latest():
    """
    263 rows của window_end mới nhất — predicted_class, proba, confidence.
    actual_label_class có thể là NULL nếu chưa evaluate (< 60 phút sau predict).
    """
    try:
        latest_we = get_latest_window_end()
        df = load_delta(GOLD_PRED_PATH, [("window_end", "=", latest_we)])

        if df.empty:
            import pandas as pd

            prev_we = str(pd.Timestamp(latest_we) - pd.Timedelta(minutes=15))
            df = load_delta(GOLD_PRED_PATH, [("window_end", "=", prev_we)])

        if df.empty:
            return {"data": [], "window_end": None}

        df["predicted_label"] = df["predicted_class"].map(CLASS_LABELS)
        if "actual_label_class" in df.columns:
            df["actual_label"] = df["actual_label_class"].map(
                lambda x: (
                    CLASS_LABELS.get(int(x), "unknown")
                    if x is not None and not isinstance(x, float)
                    else None
                )
            )

        KEEP = [
            "zone_id",
            "window_end",
            "predicted_class",
            "predicted_label",
            "pred_confidence",
            "used_model",
            "proba_0",
            "proba_1",
            "proba_2",
            "proba_3",
            "proba_4",
            "proba_5",
            "actual_label_class",
            "actual_label",
            "is_correct",
            "evaluated_at",
            "model_version",
        ]
        cols = [c for c in KEEP if c in df.columns]
        df = safe_ts(safe_ts(df[cols], "window_end"), "predicted_at")

        return {
            "data": df.to_dict(orient="records"),
            "window_end": latest_we,
            "n_zones": len(df),
        }
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/predictions/history")
def predictions_history(hours: int = Query(default=24, ge=1, le=168)):
    """
    Accuracy trend theo giờ — cho time series chart.
    Chỉ dùng rows đã có actual (evaluated_at IS NOT NULL).
    Nếu có shadow model: trả về cả production_accuracy và shadow_accuracy.
    """
    try:
        df = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", hours))
        if df.empty:
            return {"data": [], "hours": hours}

        evaluated = df[df["evaluated_at"].notna()].copy()
        if evaluated.empty:
            return {"data": [], "hours": hours, "note": "No evaluated predictions yet"}

        evaluated["window_end"] = (
            pd.to_datetime(evaluated["window_end"])
            if hasattr(evaluated["window_end"].iloc[0], "isoformat")
            else evaluated["window_end"]
        )

        # Group by giờ
        import pandas as pd

        evaluated["eval_hour"] = (
            pd.to_datetime(evaluated["window_end"]).dt.floor("h").astype(str)
        )

        agg_cols = {
            "overall_accuracy": ("is_correct", "mean"),
            "n_evaluated": ("is_correct", "count"),
            "model_a_accuracy": (
                "is_correct",
                lambda x: evaluated.loc[x.index][
                    evaluated.loc[x.index, "used_model"] == "model_a"
                ]["is_correct"].mean(),
            ),
            "model_b_accuracy": (
                "is_correct",
                lambda x: evaluated.loc[x.index][
                    evaluated.loc[x.index, "used_model"] == "model_b_fallback"
                ]["is_correct"].mean(),
            ),
        }

        # Simpler groupby
        grp = (
            evaluated.groupby("eval_hour")
            .apply(
                lambda g: {
                    "eval_hour": g["eval_hour"].iloc[0],
                    "overall_accuracy": float(g["is_correct"].mean()),
                    "n_evaluated": int(g["is_correct"].count()),
                    "model_a_accuracy": (
                        float(g[g["used_model"] == "model_a"]["is_correct"].mean())
                        if (g["used_model"] == "model_a").any()
                        else None
                    ),
                    "model_b_accuracy": (
                        float(
                            g[g["used_model"] == "model_b_fallback"][
                                "is_correct"
                            ].mean()
                        )
                        if (g["used_model"] == "model_b_fallback").any()
                        else None
                    ),
                    "shadow_accuracy": (
                        float(g["shadow_is_correct"].mean())
                        if "shadow_is_correct" in g.columns
                        and g["shadow_is_correct"].notna().any()
                        else None
                    ),
                }
            )
            .tolist()
        )

        return {"data": sorted(grp, key=lambda x: x["eval_hour"]), "hours": hours}
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/predictions/model_split")
def predictions_model_split(hours: int = Query(default=24, ge=1, le=168)):
    """
    Tỉ lệ zones dùng Model A vs Model B trong N giờ.
    Dùng cho pie chart — cho thấy weather coverage.
    """
    try:
        df = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", hours))
        if df.empty:
            return {"data": []}

        split = (
            df.groupby("used_model")
            .agg(
                count=("zone_id", "count"),
                avg_confidence=("pred_confidence", "mean"),
                avg_accuracy=("is_correct", "mean"),
            )
            .reset_index()
        )
        split["model_label"] = split["used_model"].map(
            {
                "model_a": "Model A (với weather)",
                "model_b_fallback": "Model B (fallback, không weather)",
            }
        )
        return {"data": split.fillna(0).to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/predictions/per_zone")
def predictions_per_zone(hours: int = Query(default=24, ge=1, le=72)):
    """
    Accuracy per zone trong N giờ.
    Sort theo accuracy ASC — zone nào model hay sai nhất ở đầu.
    Dùng cho table "worst predicted zones".
    """
    try:
        df = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", hours))
        if df.empty:
            return {"data": []}

        evaluated = df[df["evaluated_at"].notna()].copy()
        if evaluated.empty:
            return {"data": [], "note": "No evaluated predictions"}

        per_zone = (
            evaluated.groupby("zone_id")
            .agg(
                accuracy=("is_correct", "mean"),
                n_evaluated=("is_correct", "count"),
                avg_confidence=("pred_confidence", "mean"),
                fallback_count=(
                    "used_model",
                    lambda x: (x == "model_b_fallback").sum(),
                ),
                avg_actual_class=("actual_label_class", "mean"),
            )
            .reset_index()
            .sort_values("accuracy")
        )
        per_zone["fallback_rate"] = per_zone["fallback_count"] / per_zone["n_evaluated"]
        return {"data": per_zone.fillna(0).to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/predictions/shadow_comparison")
def predictions_shadow_comparison(hours: int = Query(default=72, ge=1, le=168)):
    """
    So sánh Production vs Shadow accuracy theo giờ.
    Trả về [] nếu không có shadow model.
    Grafana dùng để visualize shadow period decision.
    """
    try:
        df = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", hours))
        if df.empty:
            return {"data": [], "has_shadow": False}

        evaluated = df[df["evaluated_at"].notna()].copy()
        if evaluated.empty or "shadow_is_correct" not in evaluated.columns:
            return {"data": [], "has_shadow": False}

        shadow_rows = evaluated[evaluated["shadow_is_correct"].notna()]
        if shadow_rows.empty:
            return {"data": [], "has_shadow": False}

        import pandas as pd

        shadow_rows = shadow_rows.copy()
        shadow_rows["eval_hour"] = (
            pd.to_datetime(shadow_rows["window_end"]).dt.floor("h").astype(str)
        )

        comparison = (
            shadow_rows.groupby("eval_hour")
            .apply(
                lambda g: {
                    "eval_hour": g["eval_hour"].iloc[0],
                    "production_accuracy": float(g["is_correct"].mean()),
                    "shadow_accuracy": float(g["shadow_is_correct"].mean()),
                    "n_evaluated": int(len(g)),
                    "shadow_winning": float(g["shadow_is_correct"].mean())
                    >= float(g["is_correct"].mean()),
                }
            )
            .tolist()
        )
        return {
            "data": sorted(comparison, key=lambda x: x["eval_hour"]),
            "has_shadow": True,
            "hours": hours,
        }
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/predictions/class_distribution")
def predictions_class_distribution(hours: int = Query(default=24, ge=1, le=168)):
    """
    Phân phối predicted class vs actual class trong N giờ.
    Dùng để phát hiện class imbalance hoặc systematic bias.
    """
    try:
        df = load_delta(GOLD_PRED_PATH, ts_filters("predicted_at", hours))
        if df.empty:
            return {"predicted": [], "actual": []}

        pred_dist = (
            df.groupby("predicted_class")["zone_id"]
            .count()
            .reset_index()
            .rename(columns={"zone_id": "count", "predicted_class": "class"})
        )
        pred_dist["label"] = pred_dist["class"].map(CLASS_LABELS)

        evaluated = df[df["actual_label_class"].notna()]
        actual_dist = []
        if not evaluated.empty:
            actual_dist = (
                evaluated.groupby("actual_label_class")["zone_id"]
                .count()
                .reset_index()
                .rename(columns={"zone_id": "count", "actual_label_class": "class"})
            )
            actual_dist["label"] = actual_dist["class"].map(
                lambda x: CLASS_LABELS.get(int(x), "?")
            )
            actual_dist = actual_dist.to_dict(orient="records")

        return {
            "predicted": pred_dist.to_dict(orient="records"),
            "actual": actual_dist,
        }
    except Exception as e:
        return {"error": str(e)}


# ── GOLD SCHEMA INFO ──────────────────────────────────────────────────────────


@app.get("/meta/gold_schema")
def gold_schema():
    """
    Trả về schema mô tả của gold/aggregated và gold/predictions_monitoring.
    Dùng để debug hoặc document.
    """
    return {
        "gold_aggregated": {
            "grain": "(zone_id, window_end)",
            "window_type": "60-min sliding, 15-min slide",
            "n_zones": 263,
            "key_columns": [
                "zone_id",
                "window_end",
                "requests_60m",
                "requests_15m",
                "pickup_delay_mean",
                "imbalance",
                "neighbor_requests_60m",
                "temperature_2m",
                "weather_code",
                "is_weekend",
                "is_holiday",
            ],
        },
        "gold_predictions_monitoring": {
            "grain": "(zone_id, window_end)",
            "filled_at_predict": [
                "predicted_class",
                "proba_0..5",
                "pred_confidence",
                "used_model",
                "model_version",
                "predicted_at",
                "shadow_predicted_class (nullable)",
            ],
            "filled_at_evaluate": [
                "actual_imbalance",
                "actual_label_class",
                "is_correct",
                "shadow_is_correct",
                "evaluated_at",
            ],
            "evaluate_delay": "~60 phút sau predict (lead4 × 15min = 60min)",
        },
    }
