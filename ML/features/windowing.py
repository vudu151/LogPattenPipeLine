"""Dac trung theo cua so, dung chung cho train (pandas tren Parquet warm-up), job 2 (Spark agg -> pandas) va danh gia.

Mot cua so = (source, key, window_start) voi key = host hoac "ALL".
Vector dac trung cho Isolation Forest:
  [n_events, n_unknown, n_error, n_templates, entropy, ratio_unknown, ratio_error] + log1p(so lan tung mau known)
n_error dem cac dong co severity ERROR hoac FATAL (thang chung, nen so sanh duoc giua cac he dieu hanh).
Nhan cua so (chi de danh gia): "alert" neu co it nhat mot dong chen (label = inject), nguoc lai "normal".
Giu tuong thich Python 3.8.
"""
import math
from collections import Counter

import numpy as np
import pandas as pd

ERROR_SEVERITIES = {"ERROR", "FATAL"}
BASE_COLS = ["n_events", "n_unknown", "n_error", "n_templates", "entropy", "ratio_unknown", "ratio_error"]


def _is_marked(label):
    return label not in (None, "normal", "") and not (isinstance(label, float) and math.isnan(label))


def window_stats(template_ids, statuses, severities, labels, weights=None):
    """Thong ke cua MOT cua so tu cac list cung do dai. weights: so dong ma moi phan tu dai dien (mac dinh 1)."""
    if weights is None:
        weights = [1] * len(template_ids)
    n = int(sum(weights))
    cnt = Counter()
    n_unknown = n_error = 0; marked = False
    for t, s, sev, lab, w in zip(template_ids, statuses, severities, labels, weights):
        if t is not None:
            cnt[t] += w
        if s == "unknown":
            n_unknown += w
        if sev in ERROR_SEVERITIES:
            n_error += w
        marked = marked or _is_marked(lab)
    ent = -sum((c / n) * math.log2(c / n) for c in cnt.values()) if n else 0.0
    top = ";".join("%s:%d" % (t, c) for t, c in cnt.most_common(3))
    return {"n_events": n, "n_unknown": int(n_unknown), "n_error": int(n_error), "n_templates": len(cnt), "entropy": round(ent, 4),
            "ratio_unknown": n_unknown / n if n else 0.0, "ratio_error": n_error / n if n else 0.0,
            "label": "alert" if marked else "normal", "counts": cnt, "top_templates": top}


def windows_from_events(df, window_s=60, key="host"):
    """df: cot event_time (tz-aware), source, host, severity, template_id, status, label -> DataFrame cua so."""
    df = df.dropna(subset=["event_time"]).copy()
    df["window_start"] = df["event_time"].dt.floor("%ds" % window_s)
    df["key"] = df[key] if key != "ALL" else "ALL"
    rows = []
    for (src, k, ws), g in df.groupby(["source", "key", "window_start"], sort=False):
        st = window_stats(list(g["template_id"]), list(g["status"]), list(g["severity"]), list(g["label"]))
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
