# ==============================================================
# LightGBM — Full Training Pipeline
# Local Windows | 3050Ti CUDA | 16GB RAM | 6-class | Time-series
# ==============================================================
# CÀI ĐẶT (chạy 1 lần trong terminal):
#   pip install lightgbm pyarrow psutil scikit-learn numpy matplotlib
#
# CHẠY:
#   python train_lgb.py
# ==============================================================

import gc
import sys
import time
import psutil
import numpy as np
import pyarrow.parquet as pq
import lightgbm as lgb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.utils.class_weight import compute_class_weight

# ==============================================================
# CONFIG — chỉ sửa ở đây
# ==============================================================
PARQUET_PATH = r"D:\BigData-master\BigData\datasets\train_data"
OUTPUT_DIR   = r"D:\BigData-master\BigData\datasets\lgb_output"
ML_PATH      = r"D:\BigData-master\BigData\ml"   # path chứa feature_builder.py
LABEL_COL    = "label_6class"
VALID_RATIO  = 0.10
BATCH_SIZE   = 2_000_000
RANDOM_SEED  = 42
N_CLASS      = 6

# ==============================================================
# FLAG — đổi thành False để train Model B (không có weather)
# ==============================================================
USE_WEATHER = True

# ==============================================================
# WEATHER FEATURES — đồng bộ với feature_builder.py
# ==============================================================
_RAW_WEATHER_COLS = [
    "temperature_2m", "relative_humidity_2m", "surface_pressure",
    "precipitation", "rain", "snowfall",
    "cloud_cover", "weather_code", "wind_speed_10m", "wind_gusts_10m",
]
_WEATHER_LEAD_COLS = [
    f"{c}_lead{s}"
    for c in ["temperature_2m", "relative_humidity_2m", "surface_pressure", "cloud_cover", "weather_code"]
    for s in [1, 2, 3]
]

# ==============================================================
# DROP_COLS — tự động theo USE_WEATHER
# ==============================================================
DROP_COLS = {"window_end", "label_6class"}
if not USE_WEATHER:
    DROP_COLS |= set(_RAW_WEATHER_COLS) | set(_WEATHER_LEAD_COLS)

# ==============================================================
# OUTPUT — tên file tự động theo USE_WEATHER
# ==============================================================
MODEL_FILE = "lgb_final_model.txt" if USE_WEATHER else "lgb_model_b.txt"
CKPT_FILE  = "lgb_checkpoint.txt"  if USE_WEATHER else "lgb_model_b_checkpoint.txt"

# ==============================================================
# PARAMS
# ==============================================================
PARAMS = {
    "objective":         "multiclass",
    "num_class":         N_CLASS,
    "metric":            ["multi_logloss", "multi_error"],
    "device":            "cpu",
    "num_leaves":        255,
    "max_depth":         -1,
    "min_child_samples": 50,
    "feature_fraction":  0.9,
    "bagging_fraction":  0.9,
    "bagging_freq":      1,
    "reg_alpha":         0.1,
    "reg_lambda":        1.0,
    "min_split_gain":    0.001,
    "max_bin":           255,
    "learning_rate":     0.05,
    "n_jobs":            -1,
    "verbose":           -1,
    "seed":              RANDOM_SEED,
}

NUM_BOOST_ROUND = 600
EARLY_STOP      = 50
LOG_EVERY       = 50
CKPT_EVERY      = 200


# ==============================================================
# HELPERS
# ==============================================================
def ram(label=""):
    u = psutil.virtual_memory().used / 1e9
    t = psutil.virtual_memory().total / 1e9
    print(f"  [{label}] RAM: {u:.1f} / {t:.1f} GB  ({u/t*100:.0f}%)")


def elapsed(start):
    s = time.time() - start
    return f"{s/60:.1f} min" if s > 60 else f"{s:.1f}s"


class CheckpointCallback:
    def __init__(self, path, every=200):
        self.path  = path
        self.every = every
        self.order = 0
        self.before_iteration = False

    def __call__(self, env):
        if env.iteration % self.every == 0 and env.iteration > 0:
            env.model.save_model(str(self.path))
            print(f"  checkpoint saved @ iter {env.iteration}")


# ==============================================================
# MAIN
# ==============================================================
def main():
    t0  = time.time()
    out = Path(OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    ckpt_path = out / CKPT_FILE

    mode = "Model A (WITH weather)" if USE_WEATHER else "Model B (NO weather)"

    # ----------------------------------------------------------
    # 1. Đọc schema
    # ----------------------------------------------------------
    print("=" * 55)
    print(f"STEP 1 — Doc schema  [{mode}]")
    print("=" * 55)
    dataset   = pq.ParquetDataset(PARQUET_PATH)
    all_cols  = set(dataset.schema.names)

    # Import feature_builder để lấy canonical column order
    sys.path.insert(0, ML_PATH)
    from feature_builder import ALL_FEATURE_COLS, NO_WEATHER_FEATURE_COLS

    # Dùng thứ tự từ ALL_FEATURE_COLS (không từ parquet schema)
    # → đảm bảo train/inference dùng cùng position cho LightGBM Booster.predict()
    if USE_WEATHER:
        feat_cols = [c for c in ALL_FEATURE_COLS if c in all_cols]
    else:
        feat_cols = [c for c in NO_WEATHER_FEATURE_COLS if c in all_cols]

    read_cols = feat_cols + [LABEL_COL]

    print(f"  Features  : {len(feat_cols)}")
    print(f"  Label     : {LABEL_COL}")
    print(f"  In parquet but not in ALL_FEATURE_COLS: {all_cols - set(feat_cols) - {LABEL_COL}}")
    ram("schema")

    # ----------------------------------------------------------
    # 2. Stream full data
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 2 — Stream full data")
    print("=" * 55)
    t1 = time.time()
    X_parts = []
    y_parts = []
    total_rows = 0
    parquet_files = [f.path for f in dataset.fragments if str(f.path).endswith('.parquet')]
    print(f"  So file parquet: {len(parquet_files)}")

    for file_path in parquet_files:
        pf = pq.ParquetFile(file_path)
        for batch in pf.iter_batches(batch_size=BATCH_SIZE, columns=read_cols):
            tbl = batch.to_pydict()
            n   = len(tbl[LABEL_COL])
            total_rows += n

            y_b = np.array(tbl[LABEL_COL], dtype="int32")
            X_b = np.stack(
                [np.array(tbl[c], dtype="float32") for c in feat_cols],
                axis=1,
            )
            X_parts.append(X_b)
            y_parts.append(y_b)

            del tbl, X_b, y_b
            gc.collect()
            print(f"  loaded {total_rows:,} rows", end="\r")

    print(f"\n  Tong: {total_rows:,} rows  ({elapsed(t1)})")

    X = np.concatenate(X_parts); del X_parts; gc.collect()
    y = np.concatenate(y_parts); del y_parts; gc.collect()

    print(f"  Shape : {X.shape}  dtype: {X.dtype}")
    unique, counts = np.unique(y, return_counts=True)
    for cls, cnt in zip(unique, counts):
        print(f"    class {cls}: {cnt:,}  ({cnt/len(y)*100:.1f}%)")
    ram("after stream")

    # ----------------------------------------------------------
    # 3. Time-series split
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 3 — Time-series split")
    print("=" * 55)
    split        = int(len(X) * (1 - VALID_RATIO))
    X_tr, X_val  = X[:split], X[split:]
    y_tr, y_val  = y[:split], y[split:]
    del X, y; gc.collect()

    print(f"  Train : {X_tr.shape}")
    print(f"  Valid : {X_val.shape}")
    ram("after split")

    # ----------------------------------------------------------
    # 4. Class weights
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 4 — Class weights")
    print("=" * 55)
    classes = np.unique(y_tr)
    weights = compute_class_weight("balanced", classes=classes, y=y_tr)
    w_map   = dict(zip(classes.tolist(), weights.tolist()))
    for k, v in w_map.items():
        print(f"  class {k}: {v:.3f}")

    sw_tr  = np.array([w_map[c] for c in y_tr], dtype="float32")
    sw_val = np.array([w_map[c] for c in y_val], dtype="float32")

    # ----------------------------------------------------------
    # 5. Build LGB Dataset
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 5 — Build LGB Dataset")
    print("=" * 55)
    t2 = time.time()

    print("  Building dtrain...")
    dtrain = lgb.Dataset(
        X_tr, label=y_tr, feature_name=feat_cols, weight=sw_tr,
        free_raw_data=True, categorical_feature=["zone_id"],
        params={"max_bin": PARAMS["max_bin"]},
    )
    dtrain.construct()
    del X_tr, y_tr, sw_tr; gc.collect()
    ram("after dtrain")

    print("  Building dvalid...")
    dvalid = lgb.Dataset(
        X_val, label=y_val, reference=dtrain, feature_name=feat_cols, weight=sw_val,
        free_raw_data=True, categorical_feature=["zone_id"],
        params={"max_bin": PARAMS["max_bin"]},
    )
    dvalid.construct()
    del X_val, y_val, sw_val; gc.collect()
    ram(f"after dvalid — ready to train  ({elapsed(t2)})")

    # ----------------------------------------------------------
    # 6. Train
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print(f"STEP 6 — Training  [{mode}]")
    print("=" * 55)
    t3 = time.time()

    model = lgb.train(
        PARAMS, dtrain,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=[
            lgb.early_stopping(EARLY_STOP),
            lgb.log_evaluation(LOG_EVERY),
            CheckpointCallback(ckpt_path, every=CKPT_EVERY),
        ],
    )

    print(f"\n  Best iteration : {model.best_iteration}")
    print(f"  Best logloss   : {model.best_score['valid']['multi_logloss']:.4f}")
    print(f"  Best error     : {model.best_score['valid']['multi_error']:.4f}")
    print(f"  Train time     : {elapsed(t3)}")
    ram("after train")

    # ----------------------------------------------------------
    # 7. Lưu model
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 7 — Luu model")
    print("=" * 55)
    model_path = out / MODEL_FILE
    model.save_model(str(model_path))
    print(f"  Model saved : {model_path}")
    print(f"  Num features: {model.num_feature()}")


if __name__ == "__main__":
    main()