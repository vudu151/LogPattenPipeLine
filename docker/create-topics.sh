#!/bin/bash
# Tạo các topic của pipeline. Idempotent: chạy lại không lỗi nếu topic đã tồn tại.
set -e
BOOT=kafka:9092
KT=/opt/kafka/bin/kafka-topics.sh

create() {  # name partitions
  $KT --bootstrap-server $BOOT --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

echo "[kafka-init] tao topic..."
create raw-logs.bgl        4
create raw-logs.hdfs       4
create raw-logs.openstack  4
create parsed-logs         4
create pipeline-metrics    1
create rejected-logs       1     # dòng không đạt chất lượng (cách ly)
create alerts              1     # cảnh báo cho Alert System

echo "[kafka-init] danh sach topic hien co:"
$KT --bootstrap-server $BOOT --list
