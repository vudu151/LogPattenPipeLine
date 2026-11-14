#!/usr/bin/env python3
"""Warm-up: hoc cay Drain tu phan dau cua log tung he dieu hanh de xay tap mau KNOWN.

Chay trong container Spark (cung ban drain3 voi job 1):
  docker compose exec spark-master python3 /app/registry/warmup.py --dataset Linux --fraction 0.5

Log he dieu hanh khong co nhan: phan dau cua log duoc coi la lich su da biet, mau nao xuat hien sau do la "unknown".
Dong bi normalizer cach ly khong duoc hoc. Ket qua:
  - Redis: snapshot cay Drain, SET known:<SRC>, warmup:<SRC>:lines
  - PostgreSQL: bang templates (status=known, from_warmup=true)
  - lake/warmup/<SRC>.parquet: log warm-up da gan template (dung de train Isolation Forest o buoc 4)
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from drain3.redis_persistence import RedisPersistence  # noqa: E402

from lib_log_handler import SOURCES, WARMUP_FILES, handle  # noqa: E402
from registry.common import REDIS_HOST, REDIS_PORT, key, new_miner, redis_client, template_id  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PG = dict(host=os.environ.get("PG_HOST", "postgres"), port=5432, user="app", password="app", dbname="logs")


def count_lines(path):
    n = 0
    with open(path, "rb") as f:
        for buf in iter(lambda: f.read(1 << 24), b""):
            n += buf.count(b"\n")
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="Linux", choices=SOURCES)
    ap.add_argument("--file")
    ap.add_argument("--fraction", type=float, default=0.2, help="ti le dong dau dung cho warm-up")
    ap.add_argument("--lines", type=int, help="so dong warm-up (ghi de --fraction)")
    ap.add_argument("--skip-value", type=int, help="gia tri --skip ghi vao Redis (mac dinh = so dong warm-up; dat 0 khi tep phat lai khac tep warm-up)")
    args = ap.parse_args()
    src = args.dataset
    path = args.file or os.path.join(ROOT, "data", "raw", WARMUP_FILES[src])
    n_lines = args.lines or int(count_lines(path) * args.fraction)
    print("[warmup] %s: hoc %d dong dau cua %s" % (src, n_lines, path), flush=True)

    redis_client().delete(key("snapshot", src))      # bat dau tu cay rong
    miner = new_miner(RedisPersistence(REDIS_HOST, REDIS_PORT, 0, None, False, key("snapshot", src)), src)
    t0 = time.time()
    learned = rejected = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= n_lines:
                break
            action, _flags, rec = handle(src, line)
            if action == "reject":                      # dong khong dat chat luong: khong hoc, khong ghi
                rejected += 1
                continue
            miner.add_log_message(rec["content"]); learned += 1
            if learned % 200000 == 0:
                print("[warmup] da hoc %d dong, %d cluster, %.0fs" % (learned, len(miner.drain.clusters), time.time() - t0), flush=True)

    clusters = list(miner.drain.clusters)
    known = {}
    for c in clusters:
        tpl = c.get_template()
        known[template_id(src, tpl, "K")] = (tpl, c.size)
    print("[warmup] xong hoc: %d dong, cach ly %d dong loi chat luong, %d mau known, %.0fs" % (
        learned, rejected, len(known), time.time() - t0), flush=True)

    # --- luu Redis ---
    miner.save_state("warmup")
    r = redis_client()
    r.delete(key("known", src), key("promoted", src))
    if known:
        r.sadd(key("known", src), *known.keys())
    r.set(key("lines", src), n_lines if args.skip_value is None else args.skip_value)

    # --- gan mau cho toan bo dong warm-up va ghi Parquet (du lieu huan luyen Isolation Forest) ---
    # Doc lai tep lan hai thay vi giu noi dung trong bo nho (1 trieu dong Windows chiem hon 1 GB).
    d = miner.drain
    out = []; tid_of = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= n_lines:
                break
            action, _flags, rec = handle(src, line)
            if action == "reject":
                continue
            c = d.tree_search(d.root_node, d.get_content_as_tokens(miner.masker.mask(rec["content"])), d.sim_th, False)
            if c is not None:
                tid = tid_of.get(c.cluster_id)
                if tid is None:
                    tid = tid_of[c.cluster_id] = template_id(src, c.get_template(), "K")
                out.append((rec["event_time"], rec["host"], rec["severity"], None, tid, "known"))
            else:
                out.append((rec["event_time"], rec["host"], rec["severity"], None, None, "unknown"))
    df = pd.DataFrame(out, columns=["event_time", "host", "severity", "label", "template_id", "status"])
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    os.makedirs(os.path.join(ROOT, "lake", "warmup"), exist_ok=True)
    pq = os.path.join(ROOT, "lake", "warmup", "%s.parquet" % src)
    df.to_parquet(pq, index=False)
    print("[warmup] ghi %s (%d dong, %d dong khong khop mau cuoi cung)" % (
        pq, len(df), int(((df.status == "unknown")).sum())), flush=True)

    # --- PostgreSQL ---
    first, last = df["event_time"].min(), df["event_time"].max()
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM templates WHERE source = %s", (src,))
        cur.executemany(
            "INSERT INTO templates (template_id, source, template, status, first_seen, last_seen, occurrences, from_warmup) "
            "VALUES (%s,%s,%s,'known',%s,%s,%s,true)",
            [(tid, src, tpl, first.to_pydatetime(), last.to_pydatetime(), size) for tid, (tpl, size) in known.items()])
    print("[warmup] ghi %d template vao PostgreSQL. Replay tiep theo nen dung: --skip %d" % (len(known), n_lines if args.skip_value is None else args.skip_value), flush=True)


if __name__ == "__main__":
    main()
