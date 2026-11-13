"""Anh xa GAV (Global-as-View): quan he trung gian LogEvent duoc dinh nghia la khung nhin tren cac nguon.

    LogEvent  ⊇  V_BGL(BGL)  ∪  V_HDFS(HDFS)  ∪  V_OpenStack(OpenStack)        (gia thiet the gioi mo)

Moi khung nhin V_S khai bao tuong minh: voi tung thuoc tinh cua LogEvent, lay tu truong nao cua nguon va qua phep
bien doi nao. Wrapper cua mot nguon = (luoc do dich T_W = LogEvent, chuong trinh trich xuat E_W = bieu thuc chinh quy
tach dong log thanh cac truong cua luoc do nguon). Viet lai truy van trong GAV = khai trien khung nhin: `apply()`.

Giu tuong thich Python 3.8 (chay trong executor Spark).
"""
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional


# ====================== 1. Luoc do nguon va E_W (trich xuat) ======================
class SourceRelation:
    """Luoc do cua mot nguon: ten quan he, cac truong theo thu tu, va bieu thuc chinh quy tach mot dong thanh cac truong do."""

    def __init__(self, name, attributes, pattern, example):
        self.name = name; self.attributes = attributes; self.pattern = re.compile(pattern); self.example = example

    def extract(self, line):
        m = self.pattern.match(line)
        return m.groupdict() if m else None


SOURCE_RELATIONS = {
    "BGL": SourceRelation(
        "BGL", ["label", "epoch", "date", "node", "time", "node_repeat", "type", "component", "level", "content"],
        r"^(?P<label>\S+) (?P<epoch>\d+) (?P<date>\S+) (?P<node>\S+) (?P<time>\S+) "
        r"(?P<node_repeat>\S+) (?P<type>\S+) (?P<component>\S+) (?P<level>\S+) ?(?P<content>.*)$",
        "- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected"),
    "HDFS": SourceRelation(
        "HDFS", ["date", "time", "pid", "level", "component", "content"],
        r"^(?P<date>\d{6}) (?P<time>\d{6}) (?P<pid>\d+) (?P<level>\S+) (?P<component>[^:]+): ?(?P<content>.*)$",
        "081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating"),
    "OpenStack": SourceRelation(   # khoi [req-...] la tuy chon: dong traceback khong co khoi nay
        "OpenStack", ["logfile", "date", "time", "pid", "level", "component", "addr", "content"],
        r"^(?P<logfile>\S+) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}\.\d+) (?P<pid>\d+) "
        r"(?P<level>\S+) (?P<component>\S+) ?(?:(?P<addr>\[[^\]]*\]) ?)?(?P<content>.*)$",
        'nova-api.log.1.2017-05-16_13:53:08 2017-05-16 00:00:00.008 25746 INFO nova.osapi_compute.wsgi.server [req-38101a0b - - -] 10.11.10.1 "GET /v2/servers/detail HTTP/1.1" status: 200'),
}


# ====================== 2. Cac phep bien doi gia tri ======================
_BLK = re.compile(r"blk_-?\d+")
_INSTANCE = re.compile(r"\[instance: ([0-9a-f-]{36})\]")


def _iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def t_identity(x):
    return x


def t_bgl_time(time, epoch):
    try:
        return _iso(datetime.strptime(time, "%Y-%m-%d-%H.%M.%S.%f"))
    except ValueError:
        return _iso(datetime.utcfromtimestamp(int(epoch)))


def t_bgl_label(label):
    return "normal" if label == "-" else label          # BGL: '-' la binh thuong, con lai la ma canh bao


def t_hdfs_time(date, time):
    return _iso(datetime.strptime(date + time, "%y%m%d%H%M%S"))


def t_hdfs_block(content, pid):
    m = _BLK.search(content)
    return m.group(0) if m else "pid-" + pid            # thuc the cua HDFS la khoi; khong co ma khoi thi dung pid


def t_os_time(date, time):
    return _iso(datetime.strptime(date + " " + time, "%Y-%m-%d %H:%M:%S.%f"))


def t_os_instance(content, logfile):
    m = _INSTANCE.search(content)
    return m.group(1) if m else logfile.split(".log")[0]


def t_none():
    return None                                         # nhan cua HDFS / OpenStack ghep sau tu tep nhan (labels.py)


TRANSFORMS = {"identity": t_identity, "bgl_time": t_bgl_time, "bgl_label": t_bgl_label, "hdfs_time": t_hdfs_time,
              "hdfs_block": t_hdfs_block, "os_time": t_os_time, "os_instance": t_os_instance, "none": t_none}

# ====================== 3. Khung nhin GAV: thuoc tinh LogEvent <- (cac truong nguon, phep bien doi) ======================
GAV_VIEWS = {
    "BGL": [
        ("event_time", ["time", "epoch"], "bgl_time"),
        ("host", ["node"], "identity"),
        ("host2", ["node_repeat"], "identity"),          # BGL ghi ten nut hai lan; normalizer dung de kiem tra nhat quan
        ("level", ["level"], "identity"),
        ("component", ["component"], "identity"),
        ("content", ["content"], "identity"),
        ("label", ["label"], "bgl_label"),
    ],
    "HDFS": [
        ("event_time", ["date", "time"], "hdfs_time"),
        ("host", ["content", "pid"], "hdfs_block"),
        ("level", ["level"], "identity"),
        ("component", ["component"], "identity"),
        ("content", ["content"], "identity"),
        ("label", [], "none"),
    ],
    "OpenStack": [
        ("event_time", ["date", "time"], "os_time"),
        ("host", ["content", "logfile"], "os_instance"),
        ("level", ["level"], "identity"),
        ("component", ["component"], "identity"),
        ("content", ["content"], "identity"),
        ("label", [], "none"),
    ],
}
SOURCES = list(GAV_VIEWS)


def apply(source, line):
    # type: (str, str) -> Optional[Dict]
    """Khai trien khung nhin GAV cho mot dong: trich truong nguon roi tinh tung thuoc tinh LogEvent. None neu E_W khong khop."""
    fields = SOURCE_RELATIONS[source].extract(line.rstrip("\n"))
    if fields is None:
        return None
    return {target: TRANSFORMS[t](*[fields[a] for a in attrs]) for target, attrs, t in GAV_VIEWS[source]}


def datalog(source):
    # type: (str) -> str
    """In dinh nghia khung nhin duoi dang luat kieu Datalog (dung trong bao cao va tai lieu)."""
    rel = SOURCE_RELATIONS[source]
    head, body = [], []
    for target, attrs, t in GAV_VIEWS[source]:
        if target == "host2":
            continue
        if t == "identity":
            head.append(attrs[0])
        elif t == "none":
            head.append("null")
        else:
            head.append(target); body.append("%s = %s(%s)" % (target, t, ", ".join(attrs)))
    rule = "LogEvent(%s, '%s') :- %s(%s)" % (", ".join(head), source, rel.name, ", ".join(rel.attributes))
    return rule + ("".join(", " + b for b in body))


def format_raw(source, dt, host, content, level="INFO"):
    """Anh xa nguoc don gian: tao mot dong log dung dinh dang cua nguon (producer dung de chen mau la)."""
    if source == "BGL":
        return "INJECT {epoch} {d} {h} {t} {h} RAS KERNEL {lvl} {c}".format(
            epoch=int(dt.replace(tzinfo=timezone.utc).timestamp()), d=dt.strftime("%Y.%m.%d"), h=host,
            t=dt.strftime("%Y-%m-%d-%H.%M.%S.%f"), lvl=level, c=content)
    if source == "HDFS":
        return "{d} {t} 999 {lvl} dfs.DataNode$Injected: {c}".format(
            d=dt.strftime("%y%m%d"), t=dt.strftime("%H%M%S"), lvl=level, c=content)
    if source == "OpenStack":
        return "nova-compute.log.inject {d} {t} 99999 {lvl} nova.compute.injected [req-00000000-0000-0000-0000-000000000000 - - - - -] [instance: {h}] {c}".format(
            d=dt.strftime("%Y-%m-%d"), t=dt.strftime("%H:%M:%S.%f")[:-3], lvl=level, h=host, c=content)
    raise ValueError(source)


if __name__ == "__main__":
    for s in SOURCES:
        print(datalog(s)); print("   vi du:", apply(s, SOURCE_RELATIONS[s].example)); print()
