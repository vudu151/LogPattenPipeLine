#!/usr/bin/env python3
"""Danh gia phat hien mau chua biet bang DONG CHEN THU. Chay dung cac mo-dun cua pipeline (khung nhin GAV -> normalizer
-> cay Drain warm-up -> cua so -> Isolation Forest) tren phan log sau warm-up. Khong can Kafka/Spark/PostgreSQL.

  docker compose exec -e PYTHONPATH=/app spark-master python3 /app/eval/detection_metrics.py --dataset Linux
  ... --dataset Windows --lines 1000000

Log he dieu hanh cua Loghub KHONG co nhan bat thuong. Vi vay:
  - Chen N dong thuoc cac mau tu dat (log_producer/inject_templates.txt), dung dinh dang cua nguon, vao vi tri ngau nhien.
    Recall = ti le dong chen duoc gan unknown (muc dong) / ti le cua so chua dong chen bi bao (muc cua so).
  - Voi dong goc, bao cao ti le dong unknown "tu nhien" va cac mau moi xuat hien nhieu nhat: day la mau that su
    chua co trong giai doan warm-up, khong phai bao nham.
Ket qua: results/detection_<SRC>.csv, results/unknown_templates_<SRC>.csv
"""
import argparse
import csv
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from features.windowing import featurize, windows_from_events  # noqa: E402
from lib_log_handler import DEFAULT_FILES, SOURCES, format_raw, handle  # noqa: E402
from registry import online  # noqa: E402
from registry.common import key, redis_client  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def load_inject():
    with open(os.path.join(ROOT, "log_producer", "inject_templates.txt"), encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="Linux", choices=SOURCES)
    ap.add_argument("--file")
    ap.add_argument("--lines", type=int, help="so dong danh gia sau phan warm-up (mac dinh: den het tep)")
    ap.add_argument("--skip", type=int, help="mac dinh lay tu Redis (so dong da dung cho warm-up)")
    ap.add_argument("--inject-count", type=int, default=100, help="so dong chen thu")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    src = args.dataset
    skip = args.skip if args.skip is not None else int(redis_client().get(key("lines", src)) or 0)
    clf = online.get(src)
    clf._refresh_promoted = lambda: None           # doc lap voi trang thai cua he thong dang chay
    clf.promoted = set()
    path = args.file or os.path.join(ROOT, "data", "raw", DEFAULT_FILES[src])

    def read_lines():                              # doc theo luong, khong nap ca tep vao bo nho
        with open(path, encoding="utf-8", errors="replace") as f:
            for i, ln in enumerate(f):
                if i >= skip and (args.lines is None or i < skip + args.lines):
                    yield ln
    n_lines = sum(1 for _ in read_lines())
    rnd = random.Random(args.seed)
    positions = set(rnd.sample(range(n_lines), min(args.inject_count, n_lines)))
    templates = load_inject()

    t0 = time.time()
    rows = []; tpl_text = {}; n_rej = 0; k = 0; last = None
    for i, line in enumerate(read_lines()):
        if not line.strip("\r\n"):
            continue
        action, _fl, rec = handle(src, line)
        if action == "reject":
            n_rej += 1
        else:
            tid, tpl, status = clf.classify(rec["content"])
            tpl_text[tid] = tpl; last = rec
            rows.append((rec["event_time"], rec["host"], rec["severity"], None, tid, status))
        if i in positions and last is not None:    # chen mot dong mau la ngay sau dong nay, cung thoi diem va ten may
            content = templates[k % len(templates)].replace("retry 3", "retry %d" % k); k += 1
            dt = datetime.fromisoformat(last["event_time"].replace("Z", "+00:00"))
            a2, _f2, r2 = handle(src, format_raw(src, dt, last["host"], content))
            tid, tpl, status = clf.classify(r2["content"])
            rows.append((r2["event_time"], r2["host"], r2["severity"], "inject", tid, status))
    df = pd.DataFrame(rows, columns=["event_time", "host", "severity", "label", "template_id", "status"])
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    df["source"] = src
    dt_s = time.time() - t0
    inj = df["label"] == "inject"; unk = df["status"] == "unknown"
    n_orig = int((~inj).sum()); nat = df[~inj & unk]
    print("[eval] %s: %d dong sau warm-up (bo qua %d dong dau), cach ly %d, giu %d; chen %d dong; %.0fs, %.0f dong/giay" % (
        src, n_lines, skip, n_rej, n_orig, int(inj.sum()), dt_s, n_lines / max(dt_s, 1e-6)))

    out = [{"muc": "dong", "chi_so": "dong chen duoc gan unknown (recall)", "tu": int((inj & unk).sum()), "mau": int(inj.sum())},
           {"muc": "dong", "chi_so": "dong goc unknown (mau moi tu nhien)", "tu": len(nat), "mau": n_orig},
           {"muc": "mau", "chi_so": "so mau moi tu nhien", "tu": nat["template_id"].nunique(), "mau": df.loc[~inj, "template_id"].nunique()}]

    mp = os.path.join(ROOT, "models", "iforest_%s.joblib" % src)
    if os.path.exists(mp):
        m = joblib.load(mp)
        W = windows_from_events(df, m["window_s"], m["key"])
        yw = (W["label"] == "alert").values
        score = -m["model"].score_samples(featurize(W, m["template_ids"]))
        u = (W["n_unknown"] > 0).values; s = score >= m["threshold"]
        for name, pred in (("tang 1: co dong unknown", u), ("tang 2: Isolation Forest", s), ("ket hop", u | s)):
            out.append({"muc": "cua so", "chi_so": "%s | cua so co dong chen bi bao (recall)" % name, "tu": int((yw & pred).sum()), "mau": int(yw.sum())})
            out.append({"muc": "cua so", "chi_so": "%s | cua so khong co dong chen bi bao" % name, "tu": int((~yw & pred).sum()), "mau": int((~yw).sum())})
    for r in out:
        r["ti_le_pct"] = round(100.0 * r["tu"] / max(r["mau"], 1), 2)

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", "detection_%s.csv" % src), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
    pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 90)
    print(pd.DataFrame(out).to_string(index=False))

    top = Counter(nat["template_id"]).most_common(15)
    first_seen = nat.groupby("template_id")["event_time"].min()
    with open(os.path.join(ROOT, "results", "unknown_templates_%s.csv" % src), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["template_id", "lines", "first_seen", "template"])
        for tid, c in Counter(nat["template_id"]).most_common():
            w.writerow([tid, c, first_seen[tid].isoformat(), tpl_text.get(tid, "")])
    print("[eval] mau moi tu nhien xuat hien nhieu nhat:")
    for tid, c in top[:8]:
        print("   %6d  %s  %s" % (c, first_seen[tid].strftime("%Y-%m-%d"), tpl_text.get(tid, "")[:110]))


if __name__ == "__main__":
    main()
