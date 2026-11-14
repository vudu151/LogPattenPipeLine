#!/usr/bin/env python3
"""Tai log cua ba he dieu hanh trong bo Loghub (Windows, Linux, Mac) ve data/raw/<DATASET>/.

  python tools/download_datasets.py 2k          # mau 2.000 dong + dap an mau (vai giay)
  python tools/download_datasets.py Linux       # 0,2 MB nen -> 2,3 MB, 25.567 dong
  python tools/download_datasets.py Mac         # 1,5 MB nen -> 16 MB, 117.283 dong
  python tools/download_datasets.py Windows     # chi lay 5 trieu dong dau (1,2 GB) cua tep 26 GB
  python tools/download_datasets.py Windows 1000000     # hoac so dong tuy chon
  python tools/download_datasets.py all
Chi dung thu vien chuan cua Python. Tep da co thi bo qua.

Tep Windows day du nang 26 GB (114 trieu dong). Cong cu doc luong nen, ghi N dong dau roi ngat ket noi,
nen chi tai ve khoang 100 MB thay vi ca tep nen 1,7 GB.
"""
import os
import sys
import tarfile
import urllib.request

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
ZENODO = "https://zenodo.org/records/8196385/files"
GITHUB = "https://raw.githubusercontent.com/logpai/loghub/master"
DATASETS = ("Windows", "Linux", "Mac")
WINDOWS_LINES = 5000000


def _open(url):
    print("  tai:", url)
    return urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "log-pattern-pipeline"}), timeout=120)


def fetch(url, dest):
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print("  da co:", os.path.relpath(dest, RAW)); return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with _open(url) as r, open(dest + ".part", "wb") as f:
        for buf in iter(lambda: r.read(1 << 20), b""):
            f.write(buf)
    os.replace(dest + ".part", dest)


def get_2k():
    print("[2k] mau 2.000 dong + dap an mau cho Windows, Linux, Mac")
    for ds in DATASETS:
        for suffix in ("_2k.log", "_2k.log_structured.csv", "_2k.log_templates.csv"):
            fetch("%s/%s/%s%s" % (GITHUB, ds, ds, suffix), os.path.join(RAW, ds, ds + suffix))


def get_full(ds, max_lines=None):
    """Giai nen theo luong tep <ds>.log trong <ds>.tar.gz; dung lai sau max_lines dong (neu co)."""
    dest = os.path.join(RAW, ds, ds + ".log")
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print("  da co:", os.path.relpath(dest, RAW)); return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    print("[%s] %s" % (ds, "tap day du" if max_lines is None else "%d dong dau" % max_lines))
    n = 0
    with _open("%s/%s.tar.gz?download=1" % (ZENODO, ds)) as r, tarfile.open(fileobj=r, mode="r|gz") as tf:
        for member in tf:
            if not member.name.endswith(ds + ".log"):
                continue
            src = tf.extractfile(member)
            with open(dest + ".part", "wb") as out:
                for line in src:
                    out.write(line); n += 1
                    if n % 1000000 == 0:
                        print("    %d dong" % n, flush=True)
                    if max_lines is not None and n >= max_lines:
                        break
            break
    os.replace(dest + ".part", dest)
    print("  ghi %s: %d dong, %.1f MB" % (os.path.relpath(dest, RAW), n, os.path.getsize(dest) / 1e6))


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "2k"
    win_lines = int(sys.argv[2]) if len(sys.argv) > 2 else WINDOWS_LINES
    if what not in DATASETS + ("2k", "all"):
        sys.exit("dataset khong ho tro: %s (chon: 2k, %s, all)" % (what, ", ".join(DATASETS)))
    if what in ("2k", "all"):
        get_2k()
    for ds in DATASETS:
        if what in (ds, "all"):
            get_full(ds, win_lines if ds == "Windows" else None)
    print("xong. Du lieu o", os.path.normpath(RAW))
