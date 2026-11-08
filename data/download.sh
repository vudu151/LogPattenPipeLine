#!/usr/bin/env bash
# Tải dataset Loghub về data/raw/<DATASET>/ . Dùng:
#   bash data/download.sh 2k            # chỉ mẫu 2k dòng + ground truth template (vài giây)
#   bash data/download.sh BGL           # BGL đầy đủ (57 MB nén -> ~710 MB, 4.7 triệu dòng)
#   bash data/download.sh HDFS          # HDFS_v1 đầy đủ (187 MB nén -> ~1.5 GB) + nhãn block
#   bash data/download.sh OpenStack     # OpenStack (5 MB)
#   bash data/download.sh all           # 2k + BGL + HDFS + OpenStack
set -euo pipefail
cd "$(dirname "$0")"
RAW=raw; mkdir -p "$RAW"
ZENODO=https://zenodo.org/records/8196385/files
GH=https://raw.githubusercontent.com/logpai/loghub/master

fetch() { # url dest
  if [ -s "$2" ]; then echo "  da co: $2"; else echo "  tai: $1"; curl -fL --retry 3 --progress-bar -o "$2" "$1"; fi
}

do_2k() {
  echo "[2k] mau 2k dong + ground truth cho BGL, HDFS, OpenStack"
  for ds in BGL HDFS OpenStack; do
    mkdir -p "$RAW/$ds"
    fetch "$GH/$ds/${ds}_2k.log"                   "$RAW/$ds/${ds}_2k.log"
    fetch "$GH/$ds/${ds}_2k.log_structured.csv"    "$RAW/$ds/${ds}_2k.log_structured.csv"
    fetch "$GH/$ds/${ds}_2k.log_templates.csv"     "$RAW/$ds/${ds}_2k.log_templates.csv"
  done
}
do_bgl() {
  echo "[BGL] full dataset"; mkdir -p "$RAW/BGL"
  fetch "$ZENODO/BGL.zip?download=1" "$RAW/BGL/BGL.zip"
  [ -s "$RAW/BGL/BGL.log" ] || unzip -o -q "$RAW/BGL/BGL.zip" -d "$RAW/BGL"
  ls -la "$RAW/BGL"
}
do_hdfs() {
  echo "[HDFS] full dataset v1"; mkdir -p "$RAW/HDFS"
  fetch "$ZENODO/HDFS_v1.zip?download=1" "$RAW/HDFS/HDFS_v1.zip"
  [ -s "$RAW/HDFS/HDFS.log" ] || unzip -o -q "$RAW/HDFS/HDFS_v1.zip" -d "$RAW/HDFS"
  ls -la "$RAW/HDFS"
}
do_openstack() {
  echo "[OpenStack]"; mkdir -p "$RAW/OpenStack"
  fetch "$ZENODO/OpenStack.tar.gz?download=1" "$RAW/OpenStack/OpenStack.tar.gz"
  tar -xzf "$RAW/OpenStack/OpenStack.tar.gz" -C "$RAW/OpenStack"
  ls -la "$RAW/OpenStack"
}

case "${1:-2k}" in
  2k) do_2k ;;
  BGL) do_bgl ;;
  HDFS) do_hdfs ;;
  OpenStack) do_openstack ;;
  all) do_2k; do_bgl; do_hdfs; do_openstack ;;
  *) echo "dataset khong ho tro: $1"; exit 1 ;;
esac
echo "xong."
