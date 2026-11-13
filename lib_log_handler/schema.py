"""Luoc do trung gian (mediated schema) LogEvent: quan he duy nhat ma moi thanh phan phia sau lam viec.

Moi nguon log co luoc do rieng (lib_log_handler.gav.SOURCE_RELATIONS). Quan he giua luoc do nguon va LogEvent
duoc mo ta theo hai cach trong bai giang Schema Alignment:
  - GAV (Global-as-View): LogEvent dinh nghia la khung nhin tren cac nguon      -> lib_log_handler/gav.py
  - LAV (Local-as-View) : moi nguon dinh nghia la khung nhin tren LogEvent      -> lib_log_handler/lav.py
"""
from collections import OrderedDict

# thuoc tinh -> (kieu, mo ta). Nhom 1 do wrapper GAV sinh ra; nhom 2 do normalizer; nhom 3 do buoc trich mau; nhom 4 la sieu du lieu.
LOG_EVENT = OrderedDict([
    ("event_time", ("timestamp", "Thoi diem su kien, ISO 8601 UTC")),
    ("source", ("string", "Ten nguon: BGL | HDFS | OpenStack")),
    ("host", ("string", "Khoa gom / thuc the: nut (BGL), khoi (HDFS), may ao (OpenStack)")),
    ("level", ("string", "Ma muc log nguyen ban cua nguon")),
    ("component", ("string", "Thanh phan sinh log")),
    ("content", ("string", "Noi dung thong diep, dau vao cua Drain")),
    ("label", ("string", "Nhan ground truth neu co: 'normal' hoac ma bat thuong")),
    ("severity", ("string", "Muc log theo thang chung: INFO | WARN | ERROR | FATAL")),
    ("quality_flags", ("string", "Cac co chat luong, cach nhau dau phay")),
    ("template_id", ("string", "Dinh danh mau log")),
    ("template", ("string", "Mau log voi <*> thay tham so")),
    ("status", ("string", "known | unknown")),
    ("ingest_time", ("timestamp", "Thoi diem producer gui vao message queue")),
    ("processed_time", ("timestamp", "Thoi diem stream processor xu ly")),
    ("raw", ("string", "Dong log nguyen ban")),
])
WRAPPER_ATTRS = ["event_time", "host", "level", "component", "content", "label"]   # do anh xa GAV sinh ra
