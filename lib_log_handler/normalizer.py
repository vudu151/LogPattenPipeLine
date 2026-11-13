"""Log normalizer: tien xu ly va kiem tra chat luong du lieu, ap dung cho TUNG DONG sau wrapper GAV va truoc Drain.

Moi luat tra ve mot co (flag). Hanh dong tong hop cua dong:
  reject : dong khong dang tin (khong phan tich duoc, lech cot, thieu noi dung, thoi gian ngoai mien, trung lap)
           -> dua vao topic cach ly 'rejected-logs', KHONG dua vao Drain
  repair : dong dung duoc sau khi sua (dien gia tri mac dinh cho host thieu)
  ok     : dong sach
Dong thoi chuan hoa ma muc log cua tung nguon ve mot thang chung (code dictionary).
Giu tuong thich Python 3.8.
"""
from datetime import datetime

# --- tu dien ma: muc log rieng cua tung nguon -> thang chung 4 bac ---
SEVERITY = {
    "DEBUG": "INFO", "TRACE": "INFO", "AUDIT": "INFO", "INFO": "INFO",
    "WARN": "WARN", "WARNING": "WARN",
    "ERROR": "ERROR",
    "FATAL": "FATAL", "SEVERE": "FATAL", "FAILURE": "FATAL", "CRITICAL": "FATAL",
}
# --- mien gia tri hop le ---
ALLOWED_LEVELS = {
    "BGL": {"INFO", "WARNING", "ERROR", "FATAL", "SEVERE", "FAILURE"},
    "HDFS": {"INFO", "WARN", "ERROR", "FATAL", "DEBUG"},
    "OpenStack": {"INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG", "AUDIT", "TRACE"},
}
from lib_log_handler.lav import SOURCE_VIEWS

# mien thoi gian hop le cua tung nguon lay tu mo ta nguon (LAV); ngoai mien = out of range
TIME_RANGE = {name: v.time_range for name, v in SOURCE_VIEWS.items()}
MISSING_HOST = {"NULL", "UNKNOWN_LOCATION", "-", "", None}
DEFAULT_HOST = "UNKNOWN"

REJECT_FLAGS = {"unparsable", "misaligned_fields", "empty_content", "ts_out_of_range", "duplicate"}
RULES = [   # (ten luat, mo ta, hanh dong) -- dung cho bao cao
    ("unparsable", "Wrapper khong khop dinh dang cua nguon", "reject"),
    ("misaligned_fields", "Muc log ngoai danh muc hop le (dong lech cot)", "reject"),
    ("empty_content", "Thieu noi dung thong diep", "reject"),
    ("ts_out_of_range", "Thoi gian su kien ngoai mien hop le cua nguon", "reject"),
    ("duplicate", "Trung hoan toan voi mot dong da gap", "reject"),
    ("missing_host", "Thieu dinh danh nut (NULL, UNKNOWN_LOCATION)", "repair: dien UNKNOWN"),
    ("host_mismatch", "Hai truong ten nut trong cung mot dong khong khop", "flag"),
]


def check(source, rec):
    """rec: dict tu lib_log_handler.gav.apply (hoac None). Tra ve (action, flags, rec_da_sua)."""
    if rec is None:
        return "reject", ["unparsable"], None
    flags = []
    level = rec.get("level")
    if level not in ALLOWED_LEVELS.get(source, ()):
        flags.append("misaligned_fields")
    if not (rec.get("content") or "").strip():
        flags.append("empty_content")
    lo, hi = TIME_RANGE.get(source, ("1970-01-01", "2100-01-01"))
    ts = rec.get("event_time") or ""
    if not (lo <= ts[:10] <= hi):
        flags.append("ts_out_of_range")
    if rec.get("host") in MISSING_HOST:
        flags.append("missing_host")
        rec["host"] = DEFAULT_HOST                      # chinh sach gia tri thieu: dien mac dinh, giu dong
    elif rec.get("host2") is not None and rec["host2"] != rec["host"]:
        flags.append("host_mismatch")
    rec["severity"] = SEVERITY.get(level)               # None neu muc log khong hop le
    if any(f in REJECT_FLAGS for f in flags):
        return "reject", flags, rec
    return ("repair" if "missing_host" in flags else "ok"), flags, rec
