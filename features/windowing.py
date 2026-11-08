"""Dac trung theo window, dung chung cho train (pandas tren Parquet warm-up) va job 2 (Spark agg -> pandas).

Mot window = (source, key, window_start) voi key = host (BGL: node, HDFS: block_id) hoac "ALL".
Vector dac trung cho Isolation Forest:
  [n_events, n_unknown, n_error, n_templates, entropy, ratio_unknown, ratio_error] + log1p(count tung template known)
Nhan window (de danh gia): "alert" neu co it nhat mot dong co label khac normal/None, nguoc lai "normal".
Giu tuong thich Python 3.8.
"""
import math
from collections import Counter

import numpy as np
import pandas as pd

ERROR_LEVELS = {"ERROR", "FATAL", "SEVERE", "FAILURE", "CRITICAL"}
BASE_COLS = ["n_events", "n_unknown", "n_error", "n_templates", "entropy", "ratio_unknown", "ratio_error"]


def window_stats(template_ids, statuses, levels, labels):
    """Tinh thong ke cua MOT window tu 4 list cung do dai."""
    n = len(template_ids)
    cnt = Counter(t for t in template_ids if t is not None)
    n_unknown = sum(1 for s in statuses if s == "unknown")
    n_error = sum(1 for l in levels if l in ERROR_LEVELS)
    ent = -sum((c / n) * math.log2(c / n) for c in cnt.values()) if n else 0.0
    label = "alert" if any(l not in (None, "normal", "") and not (isinstance(l, float) and math.isnan(l)) for l in labels) else "normal"
    top = ";".join("%s:%d" % (t, c) for t, c in cnt.most_common(3))
    return {"n_events": n, "n_unknown": n_unknown, "n_error": n_error, "n_templates": len(cnt), "entropy": round(ent, 4),
            "ratio_unknown": n_unknown / n if n else 0.0, "ratio_error": n_error / n if n else 0.0,
            "label": label, "counts": cnt, "top_templates": top}


def windows_from_events(df, window_s=60, key="host"):
    """df: cot event_time (tz-aware), source, host, level, template_id, status, label -> DataFrame window."""
    df = df.dropna(subset=["event_time"]).copy()
    df["window_start"] = df["event_time"].dt.floor("%ds" % window_s)
    df["key"] = df[key] if key != "ALL" else "ALL"
    rows = []
    for (src, k, ws), g in df.groupby(["source", "key", "window_start"], sort=False):
        st = window_stats(list(g["template_id"]), list(g["status"]), list(g["level"]), list(g["label"]))
        st.update({"source": src, "host": k, "window_start": ws, "window_end": ws + pd.Timedelta(seconds=window_s)})
        rows.append(st)
    return pd.DataFrame(rows)


def featurize(windows, template_ids):
    """windows: DataFrame co BASE_COLS + counts (Counter). template_ids: thu tu cot co dinh (luu cung model)."""
    idx = {t: i for i, t in enumerate(template_ids)}
    X = np.zeros((len(windows), len(BASE_COLS) + len(template_ids)), dtype=np.float32)
    for r, (_, w) in enumerate(windows.iterrows()):
        X[r, :len(BASE_COLS)] = [w[c] for c in BASE_COLS]
        for t, c in w["counts"].items():
            j = idx.get(t)
            if j is not None:
                X[r, len(BASE_COLS) + j] = c
    X[:, 0:4] = np.log1p(X[:, 0:4])
    X[:, len(BASE_COLS):] = np.log1p(X[:, len(BASE_COLS):])
    return X
