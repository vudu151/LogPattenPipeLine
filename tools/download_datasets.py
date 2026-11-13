#!/usr/bin/env python3
"""Tai cac tap du lieu Loghub cua ba he thong (BGL, HDFS, OpenStack) ve data/raw/<DATASET>/.

  python tools/download_datasets.py 2k          # mau 2.000 dong + dap an mau (vai giay)
  python tools/download_datasets.py BGL         # 57 MB nen -> 743 MB
  python tools/download_datasets.py HDFS        # 187 MB nen -> 1,5 GB, kem tep nhan theo khoi
  python tools/download_datasets.py OpenStack   # 5 MB, kem tep nhan theo may ao
  python tools/download_datasets.py all
Chi dung thu vien chuan cua Python. Tep da co thi bo qua.
"""
import os
import shutil
import sys
import tarfile
import urllib.request
import zipfile

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
ZENODO = "https://zenodo.org/records/8196385/files"
GITHUB = "https://raw.githubusercontent.com/logpai/loghub/master"


def fetch(url, dest):
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print("  da co:", os.path.relpath(dest, RAW)); return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    print("  tai:", url)
    req = urllib.request.Request(url, headers={"User-Agent": "log-pattern-pipeline"})
    with urllib.request.urlopen(req, timeout=120) as r, open(dest + ".part", "wb") as f:
        total = int(r.headers.get("Content-Length") or 0); done = 0; step = 0
        while True:
            buf = r.read(1 << 20)
            if not buf:
                break
            f.write(buf); done += len(buf)
            if total and done * 10 // total > step:
                step = done * 10 // total; print("    %d%%" % (step * 10), flush=True)
    os.replace(dest + ".part", dest)


def get_2k():
    print("[2k] mau 2.000 dong + ground truth cho BGL, HDFS, OpenStack")
    for ds in ("BGL", "HDFS", "OpenStack"):
        for suffix in ("_2k.log", "_2k.log_structured.csv", "_2k.log_templates.csv"):
            fetch("%s/%s/%s%s" % (GITHUB, ds, ds, suffix), os.path.join(RAW, ds, ds + suffix))


def get_bgl():
    print("[BGL] tap day du")
    z = os.path.join(RAW, "BGL", "BGL.zip"); fetch(ZENODO + "/BGL.zip?download=1", z)
    if not os.path.exists(os.path.join(RAW, "BGL", "BGL.log")):
        zipfile.ZipFile(z).extractall(os.path.join(RAW, "BGL"))


def get_hdfs():
    print("[HDFS] tap day du v1 + anomaly_label.csv")
    z = os.path.join(RAW, "HDFS", "HDFS_v1.zip"); fetch(ZENODO + "/HDFS_v1.zip?download=1", z)
    if not os.path.exists(os.path.join(RAW, "HDFS", "HDFS.log")):
        zipfile.ZipFile(z).extractall(os.path.join(RAW, "HDFS"))


def get_openstack():
    print("[OpenStack] 3 tep log + anomaly_labels.txt")
    d = os.path.join(RAW, "OpenStack"); t = os.path.join(d, "OpenStack.tar.gz"); fetch(ZENODO + "/OpenStack.tar.gz?download=1", t)
    if not os.path.exists(os.path.join(d, "openstack_abnormal.log")):
        with tarfile.open(t) as tf:
            tf.extractall(d)
    merged = os.path.join(d, "openstack_normal_all.log")            # tep warm-up: hai tep binh thuong noi lai
    if not os.path.exists(merged):
        with open(merged, "wb") as out:
            for name in ("openstack_normal1.log", "openstack_normal2.log"):
                with open(os.path.join(d, name), "rb") as f:
                    shutil.copyfileobj(f, out)


ACTIONS = {"2k": [get_2k], "BGL": [get_bgl], "HDFS": [get_hdfs], "OpenStack": [get_openstack],
           "all": [get_2k, get_bgl, get_hdfs, get_openstack]}

if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "2k"
    if what not in ACTIONS:
        sys.exit("dataset khong ho tro: %s (chon: %s)" % (what, ", ".join(ACTIONS)))
    for fn in ACTIONS[what]:
        fn()
    print("xong. Du lieu o", os.path.normpath(RAW))
