#!/usr/bin/env python3
"""Danh gia chat luong parse tren Loghub-2k (co ground truth EventId / EventTemplate).

  docker compose exec spark-master python3 /app/eval/parsing_accuracy.py --dataset Linux

Chi so:
  GA (Grouping Accuracy): ti le dong ma nhom du doan TRUNG KHOP HOAN TOAN voi nhom ground truth
      (dinh nghia cua Zhu et al., ICSE-SEIP 2019, dung trong Loghub).
  PA (Parsing Accuracy): ti le dong co template du doan giong template ground truth sau khi chuan hoa <*>.
"""
import argparse
import csv
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd  # noqa: E402

from registry.common import new_miner  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def grouping_accuracy(truth, pred):
    t_groups, p_groups = defaultdict(set), defaultdict(set)
    for i, (t, p) in enumerate(zip(truth, pred)):
        t_groups[t].add(i); p_groups[p].add(i)
    correct = 0
    for p, idx in p_groups.items():
        t_of = {truth[i] for i in idx}
        if len(t_of) == 1 and t_groups[next(iter(t_of))] == idx:
            correct += len(idx)
    return correct / len(truth)


def norm(t):
    # token nao chua <*> (vd blk_<*>, core.<*>, <*>:<*>) deu coi la tham so; gop cac <*> lien tiep
    t = " ".join("<*>" if "<*>" in tok else tok for tok in str(t).split())
    t = re.sub(r"<\*>(\s*<\*>)+", "<*>", t)
    return re.sub(r"\s+", " ", t).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="Linux", choices=["Windows", "Linux", "Mac"])
    args = ap.parse_args()
    f = os.path.join(ROOT, "data", "raw", args.dataset, "%s_2k.log_structured.csv" % args.dataset)
    df = pd.read_csv(f)
    miner = new_miner(source=args.dataset)
    pred_ids, pred_tpl = [], []
    for content in df["Content"].astype(str):
        r = miner.add_log_message(content)
        pred_ids.append(r["cluster_id"])
    for cid in pred_ids:                                  # template cuoi cung cua moi cluster
        pred_tpl.append(miner.drain.id_to_cluster[cid].get_template())
    ga = grouping_accuracy(list(df["EventId"]), pred_ids)
    pa = sum(norm(a) == norm(b) for a, b in zip(df["EventTemplate"], pred_tpl)) / len(df)
    row = {"dataset": args.dataset, "lines": len(df), "truth_templates": df["EventId"].nunique(),
           "pred_templates": len(set(pred_ids)), "grouping_accuracy": round(ga, 4), "parsing_accuracy": round(pa, 4)}
    for k, v in row.items():
        print("%-18s %s" % (k, v))
    out = os.path.join(ROOT, "results", "parsing.csv")
    new = not os.path.exists(out)
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row)); new and w.writeheader(); w.writerow(row)
    print("-> ghi them vao results/parsing.csv")


if __name__ == "__main__":
    main()
