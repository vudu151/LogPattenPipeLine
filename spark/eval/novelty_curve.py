#!/usr/bin/env python3
"""Ti le dong unknown theo do dai giai doan warm-up: hoc mau tren x% dong dau, dem dong khong khop mau trong phan con lai.

  docker compose exec -e PYTHONPATH=/app spark-master python3 /app/eval/novelty_curve.py --dataset Linux
Dung de chon do dai warm-up cho tung nguon. Khong can Redis/Kafka. Ket qua: results/novelty_curve.csv
"""
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, SOURCES, handle  # noqa: E402
from registry.common import new_miner  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="Linux", choices=SOURCES)
    ap.add_argument("--limit", type=int, help="chi dung N dong dau cua tep")
    ap.add_argument("--fractions", default="0.1,0.2,0.5,0.8")
    args = ap.parse_args()
    src = args.dataset
    contents = []
    with open(os.path.join(ROOT, "data", "raw", DEFAULT_FILES[src]), encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if args.limit and i >= args.limit:
                break
            action, _fl, rec = handle(src, line)
            if action != "reject":
                contents.append(rec["content"])
    rows = []
    for frac in [float(x) for x in args.fractions.split(",")]:
        cut = int(len(contents) * frac)
        miner = new_miner(source=src)
        for c in contents[:cut]:
            miner.add_log_message(c)
        d = miner.drain; n_unknown = 0; novel = new_miner(source=src)
        for c in contents[cut:]:
            if d.tree_search(d.root_node, d.get_content_as_tokens(miner.masker.mask(c)), d.sim_th, False) is None:
                n_unknown += 1; novel.add_log_message(c)
        rest = len(contents) - cut
        rows.append({"dataset": src, "lines": len(contents), "warmup_fraction": frac, "warmup_lines": cut, "known_templates": len(d.clusters),
                     "rest_lines": rest, "unknown_lines": n_unknown, "unknown_pct": round(100.0 * n_unknown / max(rest, 1), 3),
                     "new_templates": len(novel.drain.clusters)})
        print("%s warm-up %3.0f%% (%d dong, %d mau known) -> con lai %d dong: %d unknown (%.2f%%), %d mau moi" % (
            src, frac * 100, cut, len(d.clusters), rest, n_unknown, 100.0 * n_unknown / max(rest, 1), len(novel.drain.clusters)), flush=True)
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = os.path.join(ROOT, "results", "novelty_curve.csv")
    new = not os.path.exists(out)
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0])); new and w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    main()
