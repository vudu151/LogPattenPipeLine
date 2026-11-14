#!/usr/bin/env python3
"""Train Isolation Forest tren window cua du lieu warm-up (lake/warmup/<SRC>.parquet).

  docker compose exec spark-master python3 /app/models/train_iforest.py --dataset Linux

- Train chi tren window "normal" (khong co dong alert).
- Nguong anomaly chon tren toan bo window warm-up (co nhan) sao cho F1 cao nhat; in ra P/R/F1 (lac quan vi la tap train).
- Luu models/iforest_<SRC>.joblib: model, danh sach template_id (thu tu cot), threshold, window_s, key.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.ensemble import IsolationForest  # noqa: E402

from features.windowing import featurize, windows_from_events  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def prf(y, pred):
    tp = int(((y == 1) & (pred == 1)).sum()); fp = int(((y == 0) & (pred == 1)).sum()); fn = int(((y == 1) & (pred == 0)).sum())
    p = tp / (tp + fp) if tp + fp else 0.0; r = tp / (tp + fn) if tp + fn else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="Linux")
    ap.add_argument("--window", type=int, default=60, help="do dai window (giay)")
    ap.add_argument("--key", default="host", choices=["host", "ALL"])
    ap.add_argument("--trees", type=int, default=200)
    ap.add_argument("--contamination", default="auto")
    ap.add_argument("--threshold", default="q0.95", help="'f1' = toi uu F1 tren warm-up; 'qX' = phan vi X cua score window normal (mac dinh q0.95)")
    args = ap.parse_args()
    src = args.dataset
    t0 = time.time()
    df = pd.read_parquet(os.path.join(ROOT, "lake", "warmup", "%s.parquet" % src))
    df["source"] = src
    print("[train] %s: %d dong warm-up -> window %ds theo %s" % (src, len(df), args.window, args.key), flush=True)
    W = windows_from_events(df, args.window, args.key)
    y = (W["label"] == "alert").astype(int).values
    print("[train] %d window, %d window alert (%.1f%%), %.0fs" % (len(W), y.sum(), 100 * y.mean(), time.time() - t0), flush=True)

    template_ids = sorted(df["template_id"].dropna().unique())
    X = featurize(W, template_ids)
    model = IsolationForest(n_estimators=args.trees, contamination=args.contamination if args.contamination == "auto" else float(args.contamination),
                            random_state=42, n_jobs=-1).fit(X[y == 0])
    score = -model.score_samples(X)                       # ~[0,1], cao = bat thuong
    best = (0.0, 0.0, 0.0, 0.5)
    for th in np.quantile(score, np.linspace(0.5, 0.999, 120)):
        p, r, f = prf(y, (score >= th).astype(int))
        if f > best[2]:
            best = (p, r, f, float(th))
    p, r, f, th_f1 = best
    print("[train] nguong toi uu F1 = %.4f  P=%.3f R=%.3f F1=%.3f (tren tap warm-up, lac quan)" % (th_f1, p, r, f), flush=True)
    if args.threshold == "f1":
        th = th_f1
    else:
        q = float(args.threshold[1:]); th = float(np.quantile(score[y == 0], q))
        p, r, f = prf(y, (score >= th).astype(int))
        print("[train] nguong chon = phan vi %.2f cua window normal = %.4f  P=%.3f R=%.3f F1=%.3f" % (q, th, p, r, f), flush=True)
    print("[train] score normal: mean=%.3f p95=%.3f | score alert: mean=%.3f" % (
        score[y == 0].mean(), np.quantile(score[y == 0], 0.95), score[y == 1].mean() if y.sum() else float("nan")), flush=True)

    os.makedirs(os.path.join(ROOT, "models"), exist_ok=True)
    out = os.path.join(ROOT, "models", "iforest_%s.joblib" % src)
    joblib.dump({"model": model, "template_ids": template_ids, "threshold": th, "window_s": args.window, "key": args.key,
                 "n_features": X.shape[1], "train_windows": int((y == 0).sum()), "trained_at": pd.Timestamp.utcnow().isoformat()}, out)
    print("[train] luu %s (%d dac trung, %.0fs)" % (out, X.shape[1], time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
