#!/usr/bin/env python3
"""Profiling nguon log (bai Sources Profiling): kich thuoc, toc do sinh log goc, nhan, so template.

  python profiling/profile.py --dataset BGL              # full file
  python profiling/profile.py --dataset BGL --sample     # file 2k
  python profiling/profile.py --dataset BGL --limit 500000
Ket qua in ra bang markdown va ghi them vao results/profile.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, SAMPLE_FILES, parse  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=["BGL", "HDFS", "OpenStack"])
    ap.add_argument("--sample", action="store_true")
    ap.add_argument("--file")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    path = args.file or os.path.join(ROOT, "data", "raw", (SAMPLE_FILES if args.sample else DEFAULT_FILES)[args.dataset])
    if not os.path.exists(path):
        sys.exit("khong thay " + path)

    n = parse_ok = alerts = 0
    hosts = set(); levels = Counter(); per_sec = Counter(); first = last = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if args.limit and n >= args.limit:
                break
            n += 1
            rec = parse(args.dataset, line.rstrip("\n"))
            if rec is None:
                continue
            parse_ok += 1
            ts = rec["event_time"]
            first = first or ts; last = ts
            per_sec[ts[:19]] += 1
            hosts.add(rec["host"]); levels[rec["level"]] += 1
            if rec["label"] and rec["label"] != "normal":
                alerts += 1
    size_mb = os.path.getsize(path) / 1e6
    t0 = datetime.fromisoformat(first.replace("Z", "+00:00")); t1 = datetime.fromisoformat(last.replace("Z", "+00:00"))
    span_s = max((t1 - t0).total_seconds(), 1)
    rates = sorted(per_sec.values())
    p99 = rates[int(len(rates) * 0.99) - 1] if rates else 0
    tmpl_file = os.path.join(ROOT, "data", "raw", args.dataset, "{}_2k.log_templates.csv".format(args.dataset))
    n_templates = sum(1 for _ in open(tmpl_file)) - 1 if os.path.exists(tmpl_file) else None

    row = {
        "dataset": args.dataset, "file": os.path.basename(path), "size_mb": round(size_mb, 1), "lines": n,
        "parse_ok_pct": round(100 * parse_ok / max(n, 1), 2), "time_span_hours": round(span_s / 3600, 1),
        "mean_rate_per_s": round(parse_ok / span_s, 3), "p99_rate_per_s": p99, "peak_rate_per_s": rates[-1] if rates else 0,
        "unique_hosts": len(hosts), "alert_lines": alerts, "alert_pct": round(100 * alerts / max(parse_ok, 1), 3),
        "levels": " ".join("{}={}".format(k, v) for k, v in levels.most_common(6)),
        "templates_2k_ground_truth": n_templates, "first_event": first, "last_event": last,
    }
    print("| Chi so | Gia tri |\n|---|---|")
    for k, v in row.items():
        print("| {} | {} |".format(k, v))
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = os.path.join(ROOT, "results", "profile.csv")
    write_header = not os.path.exists(out)
    with open(out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)
    print("-> ghi them vao", out)


if __name__ == "__main__":
    main()
