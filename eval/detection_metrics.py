#!/usr/bin/env python3
"""Danh gia phat hien ngoai tuyen: chay dung cac mo-dun cua pipeline (wrapper -> ghep nhan -> kiem tra chat luong ->
Drain warm-up -> cua so -> Isolation Forest) tren mot doan log sau warm-up. Khong can Kafka/Spark/PostgreSQL.

  docker compose exec -e PYTHONPATH=/app spark-master python3 /app/eval/detection_metrics.py --dataset BGL --lines 1000000
  ... --dataset HDFS --lines 2000000            # danh gia theo KHOI (block_id)
  ... --dataset OpenStack                       # danh gia theo MAY AO (instance id), tep openstack_abnormal.log

Don vi danh gia theo cach gan nhan cua nguon:
  BGL       : dong (nhan tung dong) va cua so 1 phut
  HDFS      : thuc the = khoi; khoi duong tinh neu tep nhan ghi Anomaly; du doan duong tinh neu khoi co >= 1 dong unknown
  OpenStack : thuc the = may ao; tuong tu
Ket qua: results/detection_<SRC>_promote<N>.csv
"""
import argparse
import csv
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from features.windowing import featurize, windows_from_events  # noqa: E402
from lib_log_handler import DEFAULT_FILES, handle  # noqa: E402
from registry import online  # noqa: E402
from registry.common import key, redis_client  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def prf(tp, fp, fn):
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def row(muc, cfg, th, y, pred):
    y = np.asarray(y, bool); pred = np.asarray(pred, bool)
    tp = int((y & pred).sum()); fp = int((~y & pred).sum()); fn = int((y & ~pred).sum()); tn = int((~y & ~pred).sum())
    p, r, f = prf(tp, fp, fn)
    return {"muc": muc, "cau_hinh": cfg, "nguong": th, "n": len(y), "duong_tinh": int(y.sum()), "tp": tp, "fp": fp,
            "fn": fn, "tn": tn, "precision": round(p, 4), "recall": round(r, 4), "f1": round(f, 4),
            "fpr": round(fp / max(fp + tn, 1), 5)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="BGL", choices=["BGL", "HDFS", "OpenStack"])
    ap.add_argument("--file")
    ap.add_argument("--lines", type=int, default=1000000, help="so dong danh gia sau phan warm-up")
    ap.add_argument("--skip", type=int, help="mac dinh lay tu Redis (so dong da dung cho warm-up)")
    ap.add_argument("--promote-after", type=int, default=0, help="mo phong thang cap unknown -> known sau N lan (0 = tat)")
    args = ap.parse_args()
    src = args.dataset
    skip = args.skip if args.skip is not None else int(redis_client().get(key("lines", src)) or 0)
    clf = online.get(src)
    clf._refresh_promoted = lambda: None           # doc lap voi trang thai promoted cua he thong dang chay
    clf.promoted = set()
    path = args.file or os.path.join(ROOT, "data", "raw", DEFAULT_FILES[src])
    t0 = time.time()
    rows = []; unk = Counter(); promoted = set(); n_read = n_rej = pos_rej = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i < skip:
                continue
            if i >= skip + args.lines:
                break
            n_read += 1
            action, _fl, rec = handle(src, line)
            if action == "reject":
                n_rej += 1
                if rec is not None and rec.get("label") not in (None, "normal"):
                    pos_rej += 1
                continue
            tid, tpl, status = clf.classify(rec["content"])
            raw_status = status
            if status == "unknown":
                if tid in promoted:
                    status = "known"
                else:
                    unk[tid] += 1
                    if args.promote_after and unk[tid] >= args.promote_after:
                        promoted.add(tid)
            rows.append((rec["event_time"], rec["host"], rec["level"], rec["label"], tid, status, raw_status))
    df = pd.DataFrame(rows, columns=["event_time", "host", "level", "label", "template_id", "status", "raw_status"])
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    df["source"] = src
    dt = time.time() - t0
    print("[eval] %s: doc %d dong (bo qua %d dong dau), cach ly %d dong (%d dong duong tinh), con %d dong; %.0fs, %.0f dong/giay" % (
        src, n_read, skip, n_rej, pos_rej, len(df), dt, n_read / max(dt, 1e-6)))
    print("[eval] mau unknown: %d mau; thang cap: %d" % (len(unk), len(promoted)))
    out = []

    if src == "BGL":
        y = (df["label"] != "normal").values
        out.append(row("dong", "tang 1 (khong thang cap)", "", y, df["raw_status"] == "unknown"))
        if args.promote_after:
            out.append(row("dong", "tang 1 (thang cap >= %d)" % args.promote_after, "", y, df["status"] == "unknown"))
        mp = os.path.join(ROOT, "models", "iforest_%s.joblib" % src)
        if os.path.exists(mp):
            m = joblib.load(mp)
            W = windows_from_events(df, m["window_s"], m["key"])
            yw = (W["label"] == "alert").values
            score = -m["model"].score_samples(featurize(W, m["template_ids"]))
            u = (W["n_unknown"] > 0).values
            out.append(row("cua so", "tang 1: co dong unknown", "", yw, u))
            ths = [("mac dinh (q0.95 warm-up)", m["threshold"]), ("q0.99 tap danh gia", float(np.quantile(score[~yw], 0.99)))]
            for name, th in ths:
                out.append(row("cua so", "tang 2: Isolation Forest", "%s = %.4f" % (name, th), yw, score >= th))
                out.append(row("cua so", "ket hop (tang 1 OR tang 2)", "%s = %.4f" % (name, th), yw, u | (score >= th)))
            print("[eval] cua so: %d, duong tinh: %d; diem trung binh duong tinh=%.3f binh thuong=%.3f" % (
                len(W), int(yw.sum()), score[yw].mean() if yw.any() else float("nan"), score[~yw].mean()))
    else:
        # ---------- muc thuc the (khoi / may ao) ----------
        e = df[df["label"].notna()]
        if src == "OpenStack":
            e = e[e["host"].str.len() == 36]            # chi cac dong co ma may ao
        g = e.groupby("host").agg(n=("status", "size"), n_unk=("raw_status", lambda s: int((s == "unknown").sum())),
                                  pos=("label", lambda s: bool((s != "normal").any())))
        out.append(row("thuc the", "tang 1: thuc the co >= 1 dong unknown", "", g["pos"].values, (g["n_unk"] > 0).values))
        y = (e["label"] != "normal").values
        out.append(row("dong (nhan ke thua tu thuc the)", "tang 1", "", y, (e["raw_status"] == "unknown").values))
        print("[eval] thuc the: %d (duong tinh %d); so dong moi thuc the: trung vi %d" % (len(g), int(g["pos"].sum()), int(g["n"].median())))

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    fn = os.path.join(ROOT, "results", "detection_%s_promote%d.csv" % (src, args.promote_after))
    with open(fn, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
    pd.set_option("display.width", 250)
    print(pd.DataFrame(out).to_string(index=False))
    if src == "BGL":
        print("[eval] nhan duong tinh:", dict(Counter(df.loc[df["label"] != "normal", "label"]).most_common(8)))


if __name__ == "__main__":
    main()
