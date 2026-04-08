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
import time
import psutil
import numpy as np
import pyarrow.parquet as pq
import lightgbm as lgb
import matplotlib

matplotlib.use("Agg")  # không cần GUI, lưu file trực tiếp
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.utils.class_weight import compute_class_weight
from sklearn.metrics import classification_report

# ==============================================================
# CONFIG — chỉ sửa ở đây
# ==============================================================
PARQUET_PATH = r"C:\D\nam4_ki2\BigData\datasets\train_data"  # folder chứa file .parquet
OUTPUT_DIR = r"C:\D\nam4_ki2\BigData\datasets\lgb_output"  # thư mục lưu kết quả
LABEL_COL = "label_6class"
DROP_COLS = {"window_end", "label_6class"}
VALID_RATIO = 0.10  # 80% train / 20% valid theo thứ tự thời gian
BATCH_SIZE = 2_000_000  # rows/batch khi stream, giảm nếu OOM
RANDOM_SEED = 42
N_CLASS = 6  # số class thực tế trong label

# ==============================================================
# PARAMS — đã chọn sẵn tối ưu cho 16GB RAM + 3050Ti + 6-class
# ==============================================================
PARAMS = {
    # Task
    "objective": "multiclass",
    "num_class": N_CLASS,
    "metric": ["multi_logloss", "multi_error"],
    # CUDA — 3050Ti
    # Nếu báo lỗi CUDA thì đổi "cuda" → "cpu" và bỏ dòng num_gpu
    "device": "cpu",
    # Tree — num_leaves=255 cho data lớn nhiều feature
    "num_leaves": 255,
    "max_depth": -1,
    "min_child_samples": 50,  # cao hơn vì full 26M rows
    # Sampling — giảm overfit, tăng tốc
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 1,
    # Regularization
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "min_split_gain": 0.001,
    # Histogram — max_bin=63 tiết kiệm RAM, đủ cho 26M rows
    "max_bin": 255,
    # Learning
    "learning_rate": 0.05,
    # System
    "n_jobs": -1,  # dùng hết core CPU
    "verbose": -1,
    "seed": RANDOM_SEED,
}

NUM_BOOST_ROUND = 600
EARLY_STOP = 50
LOG_EVERY = 50
CKPT_EVERY = 200  # lưu checkpoint mỗi N rounds


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
    """Lưu model mỗi N iterations phòng crash"""

    def __init__(self, path, every=200):
        self.path = path
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
    t0 = time.time()
    out = Path(OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    ckpt_path = out / "lgb_checkpoint.txt"

    # ----------------------------------------------------------
    # 1. Đọc schema
    # ----------------------------------------------------------
    print("=" * 55)
    print("STEP 1 — Đọc schema")
    print("=" * 55)
    dataset = pq.ParquetDataset(PARQUET_PATH)
    all_cols = dataset.schema.names
    feat_cols = [c for c in all_cols if c not in DROP_COLS]
    read_cols = feat_cols + [LABEL_COL]

    print(f"  Features  : {len(feat_cols)}")
    print(f"  Label     : {LABEL_COL}")
    print(f"  Drop cols : {DROP_COLS & set(all_cols)}")
    ram("schema")

    # ----------------------------------------------------------
    # 2. Stream toàn bộ data vào RAM — KHÔNG sample
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 2 — Stream full data")
    print("=" * 55)
    t1 = time.time()
    X_parts = []
    y_parts = []
    total_rows = 0
    parquet_files = [f.path for f in dataset.fragments]
    print(f"  Số file parquet: {len(parquet_files)}")

    for file_path in parquet_files:
        pf = pq.ParquetFile(file_path)
        for batch in pf.iter_batches(batch_size=BATCH_SIZE, columns=read_cols):
            tbl = batch.to_pydict()
            n = len(tbl[LABEL_COL])
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

    print(f"\n  Tổng: {total_rows:,} rows  ({elapsed(t1)})")

    X = np.concatenate(X_parts)
    del X_parts
    gc.collect()
    y = np.concatenate(y_parts)
    del y_parts
    gc.collect()

    print(f"  Shape : {X.shape}  dtype: {X.dtype}")
    print("  Phân phối class:")
    unique, counts = np.unique(y, return_counts=True)
    for cls, cnt in zip(unique, counts):
        print(f"    class {cls}: {cnt:,}  ({cnt/len(y)*100:.1f}%)")
    ram("after stream")

    # ----------------------------------------------------------
    # 3. Time-series split — KHÔNG shuffle
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 3 — Time-series split")
    print("=" * 55)
    split = int(len(X) * (1 - VALID_RATIO))
    X_tr, X_val = X[:split], X[split:]
    y_tr, y_val = y[:split], y[split:]
    del X, y
    gc.collect()

    print(f"  Train : {X_tr.shape}")
    print(f"  Valid : {X_val.shape}")
    ram("after split")

    # ----------------------------------------------------------
    # 4. Class weights — imbalanced
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 4 — Class weights")
    print("=" * 55)
    classes = np.unique(y_tr)
    weights = compute_class_weight("balanced", classes=classes, y=y_tr)
    w_map = dict(zip(classes.tolist(), weights.tolist()))
    for k, v in w_map.items():
        print(f"  class {k}: {v:.3f}")

    sw_tr = np.array([w_map[c] for c in y_tr], dtype="float32")
    sw_val = np.array([w_map[c] for c in y_val], dtype="float32")

    # ----------------------------------------------------------
    # 5. Build LGB Dataset — construct() ngay để free numpy
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 5 — Build LGB Dataset")
    print("=" * 55)
    t2 = time.time()

    print("  Building dtrain...")
    dtrain = lgb.Dataset(
        X_tr,
        label=y_tr,
        feature_name=feat_cols,
        weight=sw_tr,
        free_raw_data=True,
        params={"max_bin": PARAMS["max_bin"]},
    )
    dtrain.construct()
    del X_tr, y_tr, sw_tr
    gc.collect()
    ram("after dtrain")

    print("  Building dvalid...")
    dvalid = lgb.Dataset(
        X_val,
        label=y_val,
        reference=dtrain,
        feature_name=feat_cols,
        weight=sw_val,
        free_raw_data=True,
        params={"max_bin": PARAMS["max_bin"]},
    )
    dvalid.construct()
    del X_val, y_val, sw_val
    gc.collect()
    ram(f"after dvalid — ready to train  ({elapsed(t2)})")

    # ----------------------------------------------------------
    # 6. Train
    # ----------------------------------------------------------
    print("\n" + "=" * 55)
    print("STEP 6 — Training")
    print("=" * 55)
    t3 = time.time()

    model = lgb.train(
        PARAMS,
        dtrain,
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
    print("STEP 7 — Lưu model")
    print("=" * 55)
    model_path = out / "lgb_final_model.txt"
    model.save_model(str(model_path))
    print(f"  Model saved: {model_path}")


if __name__ == "__main__":
    main()
