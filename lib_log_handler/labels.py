"""Ghep nhan ground truth tu TEP NHAN RIENG vao tung dong log theo khoa (lien ket hai nguon theo dinh danh).

  HDFS      : anomaly_label.csv (BlockId,Label)  -- khoa = ma khoi blk_... (truong host cua LogEvent)
  OpenStack : anomaly_labels.txt (danh sach UUID may ao bi tiem loi) -- khoa = ma may ao
  BGL       : nhan nam san trong dong log, khong can ghep
"""
import csv
import os
import re

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
_CACHE = {}
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def _load(source):
    if source in _CACHE:
        return _CACHE[source]
    table = {}
    if source == "HDFS":
        for cand in ("HDFS/preprocessed/anomaly_label.csv", "HDFS/anomaly_label.csv"):
            p = os.path.join(ROOT, cand)
            if os.path.exists(p):
                with open(p) as f:
                    for row in csv.DictReader(f):
                        table[row["BlockId"]] = "normal" if row["Label"] == "Normal" else "anomaly"
                break
    elif source == "OpenStack":
        p = os.path.join(ROOT, "OpenStack/anomaly_labels.txt")
        if os.path.exists(p):
            for u in _UUID.findall(open(p).read()):
                table[u] = "anomaly"
    _CACHE[source] = table
    return table


def enrich(source, rec):
    """Dien rec['label'] cho HDFS/OpenStack. Khong tim thay khoa -> 'normal' (OpenStack) hoac None (HDFS, khoa la pid)."""
    if rec is None or source == "BGL":
        return rec
    t = _load(source)
    if source == "HDFS":
        rec["label"] = t.get(rec["host"])
    else:
        rec["label"] = t.get(rec["host"], "normal")
    return rec


def size(source):
    return len(_load(source))
