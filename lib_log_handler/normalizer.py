"""Log normalizer: tien xu ly va kiem tra chat luong du lieu, ap dung cho TUNG DONG sau khung nhin GAV va truoc Drain.

Moi luat tra ve mot co (flag). Hanh dong tong hop cua dong:
  reject : dong khong phai mot su kien dung duoc (khong tach duoc, dong "last message repeated", thieu noi dung,
           thoi gian ngoai mien, muc log ngoai danh muc) -> dua vao topic cach ly 'rejected-logs', KHONG dua vao Drain
  repair : dong dung duoc sau khi sua (dien component thieu, doi bi danh ten may ve ten chuan)
  ok     : dong sach
Giu tuong thich Python 3.8.
"""
import re

from lib_log_handler.sources import SOURCE_INFO

DEFAULT_COMPONENT = "unknown"
_REPEAT = re.compile(r"^(--- )?last message repeated \d+ times?( ---)?$")
_ALIAS = {s: re.compile(i["host_alias_pattern"]) for s, i in SOURCE_INFO.items() if i.get("host_alias_pattern")}

REJECT_FLAGS = {"unparsable", "repeat_marker", "empty_content", "ts_out_of_range", "invalid_level"}
REPAIR_FLAGS = {"missing_component", "host_alias"}
RULES = [   # (ten luat, mo ta, hanh dong) -- dung cho bao cao
    ("unparsable", "Wrapper khong khop dinh dang cua nguon (dong noi tiep cua thong diep nhieu dong, dong rac)", "reject"),
    ("repeat_marker", "Dong 'last message repeated N times' cua syslog: khong phai su kien", "reject"),
    ("empty_content", "Thieu noi dung thong diep", "reject"),
    ("ts_out_of_range", "Ngay khong ton tai hoac ngoai mien thoi gian cua nguon", "reject"),
    ("invalid_level", "Muc log ngoai danh muc hop le cua nguon (dong lech cot)", "reject"),
    ("missing_component", "Thieu ten tien trinh", "repair: dien 'unknown'"),
    ("host_alias", "Ten may la bi danh do DHCP cap", "repair: doi ve ten chuan"),
    ("repeated", "Trung hoan toan voi mot dong da gap (thoi gian chi chinh xac den giay)", "flag"),
]


def check(source, rec):
    """rec: dict tu lib_log_handler.gav.apply (hoac None). Tra ve (action, flags, rec_da_sua)."""
    if rec is None:
        return "reject", ["unparsable"], None
    info = SOURCE_INFO[source]
    content = (rec.get("content") or "").strip()
    if _REPEAT.match(content):
        return "reject", ["repeat_marker"], rec
    flags = []
    if not content:
        flags.append("empty_content")
    lo, hi = info["time_range"]
    ts = rec.get("event_time")
    if not ts or not (lo <= ts[:10] <= hi):
        flags.append("ts_out_of_range")
    if info["levels"] is not None and rec.get("level") not in info["levels"]:
        flags.append("invalid_level")
    if not rec.get("component"):
        flags.append("missing_component")
        rec["component"] = DEFAULT_COMPONENT            # chinh sach gia tri thieu: dien mac dinh, giu dong
    alias = _ALIAS.get(source)
    if alias is not None and alias.match(rec.get("host") or ""):
        flags.append("host_alias")
        rec["host"] = info["canonical_host"]            # mot may, nhieu ten -> mot ten chuan
    if any(f in REJECT_FLAGS for f in flags):
        return "reject", flags, rec
    return ("repair" if any(f in REPAIR_FLAGS for f in flags) else "ok"), flags, rec
