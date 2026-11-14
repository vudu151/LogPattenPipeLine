#!/usr/bin/env python3
"""Ho so chat luong du lieu cua mot nguon: ap dung khung nhin GAV + normalizer len toan bo tep, dem tung luat.

  python profiling/quality.py --dataset Linux          (them --file de chi dinh tep khac, --limit de gioi han so dong)
Ket qua in ra man hinh va ghi them vao results/quality.csv
"""
import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, SOURCES, handle, parse  # noqa: E402
from lib_log_handler.normalizer import RULES  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=SOURCES)
    ap.add_argument("--file")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    src = args.dataset
    path = args.file or os.path.join(ROOT, "data", "raw", DEFAULT_FILES[src])
    n = blank = 0; flags = Counter(); actions = Counter(); sev = Counter(); raw_levels = Counter()
    seen = set(); hosts_before = set(); hosts_after = set(); raised = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if args.limit and n >= args.limit:
                break
            if not line.strip("\r\n"):          # dong trong: Log Producer bo qua ngay tu buoc Extract
                blank += 1
                continue
            n += 1
            before = parse(src, line)
            action, fl, rec = handle(src, line)
            h = hash(line)
            if action != "reject":
                if h in seen:
                    fl = fl + ["repeated"]
                seen.add(h)
            for x in fl:
                flags[x] += 1
            actions[action] += 1
            if rec is not None and action != "reject":
                raw_levels[rec.get("level")] += 1
                sev[rec.get("severity")] += 1
                hosts_after.add(rec["host"])
                if before is not None:
                    hosts_before.add(before["host"])
                if rec.get("level") == "Info" and rec["severity"] in ("ERROR", "FATAL"):
                    raised += 1
    rows = []
    name = os.path.basename(path)
    print("== %s: %s (%d dong, bo qua %d dong trong)" % (src, name, n, blank))
    for rule, _desc, act in RULES:
        c = flags.get(rule, 0)
        rows.append({"dataset": src, "file": name, "lines": n, "rule": rule, "action": act, "count": c, "pct": round(100.0 * c / max(n, 1), 4)})
        print("  %-18s %9d  %8.4f%%  -> %s" % (rule, c, 100.0 * c / max(n, 1), act))
    for a in ("ok", "repair", "reject"):
        rows.append({"dataset": src, "file": name, "lines": n, "rule": "ACTION_" + a, "action": a,
                     "count": actions.get(a, 0), "pct": round(100.0 * actions.get(a, 0) / max(n, 1), 4)})
    kept = max(n - actions["reject"], 1)
    print("  hanh dong: ok=%d repair=%d reject=%d (%.3f%%)" % (actions["ok"], actions["repair"], actions["reject"], 100.0 * actions["reject"] / max(n, 1)))
    print("  ten may: %d ten truoc chuan hoa -> %d ten sau chuan hoa" % (len(hosts_before), len(hosts_after)))
    print("  muc log goc: %s" % dict(raw_levels.most_common(5)))
    print("  muc do theo thang chung: %s" % {k: "%d (%.2f%%)" % (v, 100.0 * v / kept) for k, v in sev.most_common()})
    print("  dong muc Info duoc nang len ERROR/FATAL theo noi dung: %d" % raised)
    for k, v in sev.items():
        rows.append({"dataset": src, "file": name, "lines": n, "rule": "SEVERITY_%s" % k, "action": "", "count": v, "pct": round(100.0 * v / kept, 4)})
    rows.append({"dataset": src, "file": name, "lines": n, "rule": "HOSTS_before", "action": "", "count": len(hosts_before), "pct": 0})
    rows.append({"dataset": src, "file": name, "lines": n, "rule": "HOSTS_after", "action": "", "count": len(hosts_after), "pct": 0})
    rows.append({"dataset": src, "file": name, "lines": n, "rule": "INFO_raised", "action": "", "count": raised, "pct": round(100.0 * raised / kept, 4)})
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = os.path.join(ROOT, "results", "quality.csv")
    new = not os.path.exists(out)
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0])); new and w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    main()
