#!/usr/bin/env python3
"""Ho so nguon (source profiling): kich thuoc, khoang thoi gian, toc do sinh log, va do day du cua tung thuoc tinh.

  python profiling/profile.py --dataset Linux              # tep day du
  python profiling/profile.py --dataset Linux --sample     # tep 2k
  python profiling/profile.py --dataset Windows --limit 500000
Ket qua in ra bang markdown va ghi them vao results/profile.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, SAMPLE_FILES, SOURCE_INFO, SOURCES, parse  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=SOURCES)
    ap.add_argument("--sample", action="store_true")
    ap.add_argument("--file")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    src = args.dataset
    path = args.file or os.path.join(ROOT, "data", "raw", (SAMPLE_FILES if args.sample else DEFAULT_FILES)[src])
    if not os.path.exists(path):
        sys.exit("khong thay " + path)

    n = blank = parse_ok = 0
    hosts = set(); comps = set(); levels = Counter(); per_sec = Counter(); first = last = None
    have = Counter()          # so dong co gia tri cho tung thuoc tinh (truoc normalizer)
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if args.limit and n >= args.limit:
                break
            if not line.strip("\r\n"):
                blank += 1
                continue
            n += 1
            rec = parse(src, line)
            if rec is None or not rec["event_time"]:
                continue
            parse_ok += 1
            ts = rec["event_time"]
            first = min(first or ts, ts); last = max(last or ts, ts)
            per_sec[ts[:19]] += 1
            hosts.add(rec["host"]); comps.add(rec["component"]); levels[rec["level"]] += 1
            for a in ("host", "component", "pid", "level"):
                have[a] += rec[a] is not None
    size_mb = os.path.getsize(path) / 1e6
    t0 = datetime.fromisoformat(first.replace("Z", "+00:00")); t1 = datetime.fromisoformat(last.replace("Z", "+00:00"))
    span_s = max((t1 - t0).total_seconds(), 1)
    rates = sorted(per_sec.values())
    tmpl_file = os.path.join(ROOT, "data", "raw", src, "{}_2k.log_templates.csv".format(src))
    n_templates = sum(1 for _ in open(tmpl_file)) - 1 if os.path.exists(tmpl_file) else None
    native = SOURCE_INFO[src]["native"]

    def completeness(a):
        return "khong co trong nguon" if a not in native else "%.1f%%" % (100.0 * have[a] / max(parse_ok, 1))

    row = {
        "dataset": src, "file": os.path.basename(path), "size_mb": round(size_mb, 1), "lines": n, "blank_lines": blank,
        "parse_ok_pct": round(100 * parse_ok / max(n, 1), 2), "first_event": first, "last_event": last,
        "time_span_days": round(span_s / 86400, 1), "active_seconds": len(per_sec),
        "mean_rate_per_s": round(parse_ok / span_s, 4), "peak_rate_per_s": rates[-1] if rates else 0,
        "host_names": len(hosts), "components": len(comps),
        "has_host": completeness("host"), "has_component": completeness("component"),
        "has_pid": completeness("pid"), "has_level": completeness("level"),
        "levels": " ".join("{}={}".format(k, v) for k, v in levels.most_common(4)),
        "templates_2k_ground_truth": n_templates,
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
