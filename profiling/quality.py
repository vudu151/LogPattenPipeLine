#!/usr/bin/env python3
"""Ho so chat luong du lieu cua mot nguon: ap dung lib_log_handler.normalizer len toan bo tep, dem tung luat.

  python profiling/quality.py --dataset BGL          (them --file de chi dinh tep khac)
Ket qua in ra man hinh va ghi them vao results/quality.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, handle  # noqa: E402
from lib_log_handler.normalizer import RULES  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=["BGL", "HDFS", "OpenStack"])
    ap.add_argument("--file")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    src = args.dataset
    path = args.file or os.path.join(ROOT, "data", "raw", DEFAULT_FILES[src])
    n = 0; flags = Counter(); actions = Counter(); sev = Counter(); raw_levels = Counter()
    seen = set(); alert_rejected = 0; alert_total = 0; labeled = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if args.limit and n >= args.limit:
                break
            n += 1
            line = line.rstrip("\n")
            action, fl, rec = handle(src, line)
            h = hash(line)
            if h in seen:
                fl = fl + ["duplicate"]; action = "reject"
            else:
                seen.add(h)
            for x in fl:
                flags[x] += 1
            actions[action] += 1
            if rec is not None:
                raw_levels[rec.get("level")] += 1
                if action != "reject":
                    sev[rec.get("severity")] += 1
                if rec.get("label") is not None:
                    labeled += 1
                    if rec["label"] != "normal":
                        alert_total += 1
                        if action == "reject":
                            alert_rejected += 1
    rows = []
    print("== %s: %s (%d dong)" % (src, os.path.basename(path), n))
    for name, desc, act in RULES:
        c = flags.get(name, 0)
        rows.append({"dataset": src, "file": os.path.basename(path), "lines": n, "rule": name, "action": act,
                     "count": c, "pct": round(100.0 * c / max(n, 1), 4)})
        print("  %-18s %9d  %7.4f%%  -> %s" % (name, c, 100.0 * c / max(n, 1), act))
    for a in ("ok", "repair", "reject"):
        rows.append({"dataset": src, "file": os.path.basename(path), "lines": n, "rule": "ACTION_" + a, "action": a,
                     "count": actions.get(a, 0), "pct": round(100.0 * actions.get(a, 0) / max(n, 1), 4)})
    print("  hanh dong: ok=%d repair=%d reject=%d (%.3f%%)" % (actions["ok"], actions["repair"], actions["reject"],
                                                           100.0 * actions["reject"] / max(n, 1)))
    print("  muc log goc (%d gia tri khac nhau): %s" % (len(raw_levels), dict(raw_levels.most_common(8))))
    print("  thang chung sau chuan hoa: %s" % dict(sev.most_common()))
    print("  dong co nhan: %d; dong bat thuong: %d; bat thuong bi cach ly: %d" % (labeled, alert_total, alert_rejected))
    rows.append({"dataset": src, "file": os.path.basename(path), "lines": n, "rule": "ALERT_rejected", "action": "",
                 "count": alert_rejected, "pct": round(100.0 * alert_rejected / max(alert_total, 1), 4)})
    for k, v in sev.items():
        rows.append({"dataset": src, "file": os.path.basename(path), "lines": n, "rule": "SEVERITY_%s" % k, "action": "",
                     "count": v, "pct": round(100.0 * v / max(n, 1), 4)})
    out = os.path.join(ROOT, "results", "quality.csv")
    new = not os.path.exists(out)
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0])); new and w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    main()
