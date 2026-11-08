"""Header parser cho tung nguon log Loghub -> cac truong cua mediated schema (schemas/log_event.json).

Dung chung cho replayer (lay event_time) va Spark job 1 (parse day du).
Giu tuong thich Python 3.8 vi chay trong container Spark.
"""
import re
from datetime import datetime, timezone
from typing import Dict, Optional

# ---------- BGL ----------
# - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected
_BGL = re.compile(
    r"^(?P<label>\S+) (?P<epoch>\d+) (?P<date>\S+) (?P<node>\S+) (?P<time>\S+) "
    r"(?P<node_repeat>\S+) (?P<type>\S+) (?P<component>\S+) (?P<level>\S+) ?(?P<content>.*)$"
)
_BGL_TIME = "%Y-%m-%d-%H.%M.%S.%f"

# ---------- HDFS ----------
# 081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating
_HDFS = re.compile(
    r"^(?P<date>\d{6}) (?P<time>\d{6}) (?P<pid>\d+) (?P<level>\S+) (?P<component>[^:]+): ?(?P<content>.*)$"
)
_BLK = re.compile(r"blk_-?\d+")

# ---------- OpenStack ----------
# nova-api.log.1.2017-05-16_13:53:08 2017-05-16 00:00:00.008 25746 INFO nova.osapi_compute.wsgi.server [req-... ...] 10.11.10.1 "GET ..." status: 200 len: 1893 time: 0.2477829
_OPENSTACK = re.compile(
    r"^(?P<logfile>\S+) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}\.\d+) (?P<pid>\d+) "
    r"(?P<level>\S+) (?P<component>\S+) (?P<addr>\[[^\]]*\]) ?(?P<content>.*)$"
)
_INSTANCE = re.compile(r"\[instance: ([0-9a-f-]{36})\]")


def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_bgl(line: str) -> Optional[Dict]:
    m = _BGL.match(line)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group("time"), _BGL_TIME)
    except ValueError:
        dt = datetime.utcfromtimestamp(int(m.group("epoch")))
    label = m.group("label")
    return {
        "event_time": _iso(dt),
        "host": m.group("node"),
        "level": m.group("level"),
        "component": m.group("component"),
        "content": m.group("content"),
        "label": "normal" if label == "-" else label,   # BGL: '-' la binh thuong, con lai la ma alert
    }


def parse_hdfs(line: str) -> Optional[Dict]:
    m = _HDFS.match(line)
    if not m:
        return None
    dt = datetime.strptime(m.group("date") + m.group("time"), "%y%m%d%H%M%S")
    blk = _BLK.search(m.group("content"))
    return {
        "event_time": _iso(dt),
        "host": blk.group(0) if blk else "pid-" + m.group("pid"),  # gom theo block_id (nhan HDFS gan theo block)
        "level": m.group("level"),
        "component": m.group("component"),
        "content": m.group("content"),
        "label": None,                                              # nhan join sau tu anomaly_label.csv
    }


def parse_openstack(line: str) -> Optional[Dict]:
    m = _OPENSTACK.match(line)
    if not m:
        return None
    dt = datetime.strptime(m.group("date") + " " + m.group("time"), "%Y-%m-%d %H:%M:%S.%f")
    inst = _INSTANCE.search(m.group("content"))
    return {
        "event_time": _iso(dt),
        "host": inst.group(1) if inst else m.group("logfile").split(".log")[0],
        "level": m.group("level"),
        "component": m.group("component"),
        "content": m.group("content"),
        "label": None,
    }


PARSERS = {"BGL": parse_bgl, "HDFS": parse_hdfs, "OpenStack": parse_openstack}
DEFAULT_FILES = {"BGL": "BGL/BGL.log", "HDFS": "HDFS/HDFS.log", "OpenStack": "OpenStack/openstack_normal2.log"}
SAMPLE_FILES = {"BGL": "BGL/BGL_2k.log", "HDFS": "HDFS/HDFS_2k.log", "OpenStack": "OpenStack/OpenStack_2k.log"}


def parse(source: str, line: str) -> Optional[Dict]:
    return PARSERS[source](line.rstrip("\n"))


def format_raw(source: str, dt: datetime, host: str, content: str, level: str = "INFO") -> str:
    """Tao mot dong log gia dung format cua nguon, dung de tiem template la vao stream."""
    if source == "BGL":
        return "INJECT {epoch} {d} {h} {t} {h} RAS KERNEL {lvl} {c}".format(   # nhan INJECT: eval tinh la bat thuong
            epoch=int(dt.replace(tzinfo=timezone.utc).timestamp()), d=dt.strftime("%Y.%m.%d"), h=host,
            t=dt.strftime("%Y-%m-%d-%H.%M.%S.%f"), lvl=level, c=content)
    if source == "HDFS":
        return "{d} {t} 999 {lvl} dfs.DataNode$Injected: {c}".format(
            d=dt.strftime("%y%m%d"), t=dt.strftime("%H%M%S"), lvl=level, c=content)
    if source == "OpenStack":
        return "nova-compute.log.inject {d} {t} 99999 {lvl} nova.compute.injected [req-00000000-0000-0000-0000-000000000000 - - - - -] [instance: {h}] {c}".format(
            d=dt.strftime("%Y-%m-%d"), t=dt.strftime("%H:%M:%S.%f")[:-3], lvl=level, h=host, c=content)
    raise ValueError(source)
