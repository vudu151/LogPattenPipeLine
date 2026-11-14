"""Anh xa GAV (Global-as-View): quan he trung gian LogEvent duoc dinh nghia la khung nhin tren cac nguon.

    LogEvent  ⊇  V_Windows(Windows)  ∪  V_Linux(Linux)  ∪  V_Mac(Mac)        (gia thiet the gioi mo)

Day la buoc TRANSFORM cua he thong. Moi khung nhin V_S khai bao tuong minh: voi tung thuoc tinh cua LogEvent,
lay tu truong nao cua nguon va qua phep bien doi nao. Wrapper cua mot nguon = (luoc do dich T_W = LogEvent,
chuong trinh trich xuat E_W = bieu thuc chinh quy tach dong log thanh cac truong cua luoc do nguon).

Ba he dieu hanh ghi log khac nhau o nhung diem sau, va khung nhin phai bu lai:
  - Windows ghi gio dia phuong, co muc log, KHONG ghi ten may va ma tien trinh.
  - Linux va Mac (syslog) KHONG ghi nam, KHONG ghi mui gio, KHONG co muc log.
Giu tuong thich Python 3.8 (chay trong executor Spark).
"""
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from lib_log_handler.sources import SOURCE_INFO


# ====================== 1. Luoc do nguon va E_W (trich xuat) ======================
class SourceRelation:
    """Luoc do cua mot nguon: ten quan he, cac truong theo thu tu, va bieu thuc chinh quy tach mot dong thanh cac truong do."""

    def __init__(self, name, attributes, pattern, example):
        self.name = name; self.attributes = attributes; self.pattern = re.compile(pattern); self.example = example

    def extract(self, line):
        m = self.pattern.match(line.lstrip("﻿").rstrip("\r\n"))     # tep Windows bat dau bang BOM va dung CRLF
        return m.groupdict() if m else None


_SYSLOG = (r"^(?P<month>[A-Z][a-z]{2}) {1,2}(?P<day>\d{1,2}) (?P<time>\d{2}:\d{2}:\d{2}) (?P<host>\S+) "
           r"(?:(?P<component>[^\[\]:]+?)(?:\[(?P<pid>\d+)\])?%s: ?)?(?P<content>.*)$")

SOURCE_RELATIONS = {
    "Windows": SourceRelation(
        "Windows", ["date", "time", "level", "component", "content"],
        r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}), (?P<level>\S+)\s+(?P<component>\S+)\s+(?P<content>.*)$",
        "2016-09-28 04:30:31, Info                  CBS    Ending TrustedInstaller initialization."),
    "Linux": SourceRelation(
        "Linux", ["month", "day", "time", "host", "component", "pid", "content"],
        _SYSLOG % "",
        "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4"),
    "Mac": SourceRelation(     # giong syslog cua Linux, them khoi (dia chi) tuy chon sau ma tien trinh
        "Mac", ["month", "day", "time", "host", "component", "pid", "address", "content"],
        _SYSLOG % r"(?: \((?P<address>[^)]*)\))?",
        "Jul  1 09:01:05 calvisitor-10-105-160-95 com.apple.CDScheduler[43]: Thermal pressure state: 1 Memory pressure state: 0"),
}

# ====================== 2. Cac phep bien doi gia tri ======================
_MONTHS = {m: i + 1 for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split())}
SEVERITY_ORDER = ["INFO", "WARN", "ERROR", "FATAL"]
# tu dien ma: muc log rieng cua nguon -> thang chung
LEVEL_TO_SEVERITY = {"INFO": "INFO", "WARNING": "WARN", "WARN": "WARN", "ERROR": "ERROR"}
# luat suy muc do tu noi dung, dung cho nguon khong co muc log va de nang muc khi noi dung bao loi
_KEYWORDS = [
    ("FATAL", re.compile(r"\b(panic|fatal|out of memory|oom-killer|segfault|kernel bug|corrupt(ed|ion)?)\b", re.I)),
    ("ERROR", re.compile(r"\b(errors?|fail(ed|ure|ures|s)?|denied|refused|cannot|unable to|invalid|illegal|timed out|timeout|"
                         r"killed|abort(ed|ing)?|crash(ed)?|exception)\b|HRESULT = 0x8", re.I)),
    ("WARN", re.compile(r"\b(warn(ing)?|retry(ing)?|deprecated|not found|unknown|disconnect(ed)?|link down|dropped|"
                        r"unexpected|mismatch|conflict)\b", re.I)),
]


def _iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def t_identity(x):
    return x


def t_null():
    return None                                         # nguon khong co thuoc tinh nay


def t_const(value):
    return value                                        # mot gia tri co dinh cho ca nguon


def t_int(x):
    return int(x) if x not in (None, "") else None


def t_local_time(date, time, tz_offset_hours):
    """Gio dia phuong co du ngay thang nam -> UTC."""
    try:
        return _iso(datetime.strptime(date + " " + time, "%Y-%m-%d %H:%M:%S") - timedelta(hours=tz_offset_hours))
    except ValueError:
        return None


def t_syslog_time(month, day, time, tz_offset_hours, time_range):
    """Syslog khong ghi nam: chon nam sao cho ngay nam trong mien thoi gian cua nguon, roi doi sang UTC.

    None neu khong nam nao hop le (ngay khong ton tai hoac ngoai mien) -> normalizer danh dau ts_out_of_range.
    """
    lo, hi = time_range
    mo = _MONTHS.get(month)
    if mo is None:
        return None
    for year in range(int(lo[:4]), int(hi[:4]) + 1):
        try:
            local = datetime.strptime("%d-%02d-%02d %s" % (year, mo, int(day), time), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if lo <= local.strftime("%Y-%m-%d") <= hi:
            return _iso(local - timedelta(hours=tz_offset_hours))
    return None


def t_severity(level, content):
    """Muc do theo thang chung = muc cao hon giua (muc log cua nguon qua tu dien ma) va (muc suy tu noi dung)."""
    base = LEVEL_TO_SEVERITY.get((level or "").upper(), "INFO")
    inferred = "INFO"
    for sev, pat in _KEYWORDS:
        if pat.search(content or ""):
            inferred = sev
            break
    return max(base, inferred, key=SEVERITY_ORDER.index)


TRANSFORMS = {"identity": t_identity, "null": t_null, "const": t_const, "int": t_int,
              "local_time": t_local_time, "syslog_time": t_syslog_time, "severity": t_severity}


# ====================== 3. Khung nhin GAV ======================
# Moi dong: (thuoc tinh LogEvent, cac truong nguon, phep bien doi, tham so co dinh lay tu so dang ky nguon)
def _view(source):
    info = SOURCE_INFO[source]
    tz = info["tz_offset_hours"]
    if source == "Windows":
        return [
            ("event_time", ["date", "time"], "local_time", {"tz_offset_hours": tz}),
            ("host", [], "const", {"value": info["default_host"]}),
            ("component", ["component"], "identity", {}),
            ("pid", [], "null", {}),
            ("level", ["level"], "identity", {}),
            ("severity", ["level", "content"], "severity", {}),
            ("content", ["content"], "identity", {}),
        ]
    return [    # Linux va Mac cung dung syslog
        ("event_time", ["month", "day", "time"], "syslog_time", {"tz_offset_hours": tz, "time_range": info["time_range"]}),
        ("host", ["host"], "identity", {}),
        ("component", ["component"], "identity", {}),
        ("pid", ["pid"], "int", {}),
        ("level", [], "null", {}),
        ("severity", ["level", "content"], "severity", {}),      # level = null -> chi suy tu noi dung
        ("content", ["content"], "identity", {}),
    ]


GAV_VIEWS = {s: _view(s) for s in SOURCE_INFO}
SOURCES = list(GAV_VIEWS)


def apply(source, line):
    # type: (str, str) -> Optional[Dict]
    """Khai trien khung nhin GAV cho mot dong: trich truong nguon roi tinh tung thuoc tinh LogEvent. None neu E_W khong khop."""
    fields = SOURCE_RELATIONS[source].extract(line)
    if fields is None:
        return None
    return {target: TRANSFORMS[t](*[fields.get(a) for a in attrs], **params) for target, attrs, t, params in GAV_VIEWS[source]}


def datalog(source):
    # type: (str) -> str
    """In dinh nghia khung nhin duoi dang luat kieu Datalog (dung trong bao cao va tai lieu)."""
    rel = SOURCE_RELATIONS[source]
    head, body = [], []
    for target, attrs, t, params in GAV_VIEWS[source]:
        if t == "identity":
            head.append(attrs[0])
        elif t == "null":
            head.append("null")
        elif t == "const":
            head.append("'%s'" % params["value"])
        else:
            args = [a if a in rel.attributes else "null" for a in attrs]
            if "tz_offset_hours" in params:
                args.append("UTC%+d" % params["tz_offset_hours"])
            head.append(target); body.append("%s = %s(%s)" % (target, t, ", ".join(args)))
    rule = "LogEvent(%s, '%s') :- %s(%s)" % (", ".join(head), source, rel.name, ", ".join(rel.attributes))
    return rule + ("".join(", " + b for b in body))


def format_raw(source, dt, host, content, level="Error"):
    """Anh xa nguoc: tao mot dong log dung dinh dang cua nguon tu thoi diem UTC `dt` (producer dung de chen mau la)."""
    local = dt.replace(tzinfo=None) + timedelta(hours=SOURCE_INFO[source]["tz_offset_hours"])
    if source == "Windows":
        return "{d}, {lvl:<22}CBS    {c}".format(d=local.strftime("%Y-%m-%d %H:%M:%S"), lvl=level, c=content)
    return "{mon} {day:>2} {t} {h} injected[4242]: {c}".format(
        mon=local.strftime("%b"), day=local.day, t=local.strftime("%H:%M:%S"), h=host, c=content)


if __name__ == "__main__":
    for s in SOURCES:
        print(datalog(s)); print("   vi du:", apply(s, SOURCE_RELATIONS[s].example)); print()
