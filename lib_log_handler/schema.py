"""Luoc do trung gian (mediated schema) LogEvent: quan he duy nhat ma moi thanh phan phia sau lam viec.

Moi he dieu hanh ghi log theo luoc do rieng (lib_log_handler.gav.SOURCE_RELATIONS). Quan he giua luoc do nguon va
LogEvent duoc mo ta theo hai cach trong bai giang Schema Alignment:
  - GAV (Global-as-View): LogEvent dinh nghia la khung nhin tren cac nguon      -> lib_log_handler/gav.py
  - LAV (Local-as-View) : moi nguon dinh nghia la khung nhin tren LogEvent      -> lib_log_handler/lav.py
"""
from collections import OrderedDict

# thuoc tinh -> (kieu, mo ta). Nhom 1 do khung nhin GAV sinh ra; nhom 2 do normalizer; nhom 3 do buoc trich mau; nhom 4 la sieu du lieu.
LOG_EVENT = OrderedDict([
    ("event_time", ("timestamp", "Thoi diem su kien, ISO 8601 UTC")),
    ("source", ("string", "Ten nguon: Windows | Linux | Mac")),
    ("host", ("string", "Ten may sinh log (ten chuan sau khi gop bi danh)")),
    ("component", ("string", "Tien trinh / thanh phan sinh log")),
    ("pid", ("int", "Ma tien trinh; null neu nguon khong ghi")),
    ("level", ("string", "Muc log nguyen ban cua nguon; null neu nguon khong co")),
    ("severity", ("string", "Muc log theo thang chung: INFO | WARN | ERROR | FATAL")),
    ("content", ("string", "Noi dung thong diep, dau vao cua Drain")),
    ("quality_flags", ("string", "Cac co chat luong, cach nhau dau phay")),
    ("template_id", ("string", "Dinh danh mau log")),
    ("template", ("string", "Mau log voi <*> thay tham so")),
    ("status", ("string", "known | unknown")),
    ("ingest_time", ("timestamp", "Thoi diem Log Producer gui vao message queue (Extract)")),
    ("processed_time", ("timestamp", "Thoi diem stream processor xu ly (Transform)")),
    ("raw", ("string", "Dong log nguyen ban")),
])
WRAPPER_ATTRS = ["event_time", "host", "component", "pid", "level", "severity", "content"]   # do khung nhin GAV sinh ra
