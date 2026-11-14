"""Mo ta nguon kieu LAV (Local-as-View): moi nguon duoc dinh nghia la mot khung nhin tren luoc do trung gian.

    S_Linux(event_time, host, component, pid, content)  ⊆  π σ[source='Linux' ∧ event_time ∈ ...](LogEvent)   (the gioi mo)

Khac voi GAV (noi cach TINH LogEvent tu nguon), mo ta LAV noi mot nguon CHUA GI theo ngon ngu cua luoc do trung gian:
thuoc tinh nao co san trong dong log, ho he dieu hanh, mien thoi gian. Dung de chon nguon tra loi mot truy van
va viet lai truy van thanh SQL tren kho du lieu. Vi cac khung nhin o day chi la phep chon va chieu tren mot quan he,
viec viet lai rut ve kiem tra bao ham thuoc tinh va tinh thoa duoc cua dieu kien.
"""
from typing import Dict, List, Optional

from lib_log_handler.schema import LOG_EVENT, WRAPPER_ATTRS
from lib_log_handler.sources import SOURCE_INFO


class SourceView:
    def __init__(self, source, info):
        self.source = source                        # hang so trong dieu kien chon: source = '<ten>'
        self.native = set(info["native"])           # thuoc tinh LogEvent co san trong dong log cua nguon
        self.os_family = info["os_family"]          # windows | unix
        self.time_range = info["time_range"]        # (tu, den) ISO date
        self.tz_offset_hours = info["tz_offset_hours"]
        self.note = info["note"]

    def definition(self):
        return "S_%s(%s) ⊆ σ[source='%s' ∧ event_time ∈ [%s, %s]](LogEvent)" % (
            self.source, ", ".join(a for a in WRAPPER_ATTRS if a in self.native), self.source, self.time_range[0], self.time_range[1])


SOURCE_VIEWS = {s: SourceView(s, i) for s, i in SOURCE_INFO.items()}


def select_sources(attributes=(), where=None):
    # type: (List[str], Optional[Dict]) -> List[str]
    """Tra ve cac nguon co the dong gop cau tra loi cho truy van  π[attributes] σ[where] (LogEvent).

    where: dict dieu kien; khoa: source, os_family, native (danh sach thuoc tinh phai CO SAN trong dong log),
    time_from, time_to. Mot nguon bi loai khi dieu kien mau thuan voi mo ta cua nguon (khong thoa duoc).
    """
    where = where or {}
    if not set(attributes) <= set(LOG_EVENT):
        return []
    out = []
    for name, v in SOURCE_VIEWS.items():
        if "source" in where and where["source"] != name:
            continue
        if "os_family" in where and where["os_family"] != v.os_family:
            continue
        if not set(where.get("native", ())) <= v.native:
            continue
        if "time_from" in where and where["time_from"] > v.time_range[1]:
            continue
        if "time_to" in where and where["time_to"] < v.time_range[0]:
            continue
        out.append(name)
    return out


def rewrite(attributes, where=None, table="logs.log_events"):
    # type: (List[str], Optional[Dict], str) -> str
    """Viet lai truy van tren LogEvent thanh SQL tren kho du lieu, chi quet cac nguon lien quan."""
    where = where or {}
    srcs = select_sources(attributes, where)
    if not srcs:
        return "-- khong nguon nao tra loi duoc truy van nay"
    conds = []
    if "time_from" in where: conds.append("event_time >= '%s'" % where["time_from"])
    if "time_to" in where: conds.append("event_time < '%s'" % where["time_to"])
    extra = "".join(" AND " + c for c in conds)
    return "\nUNION ALL\n".join("SELECT %s FROM %s WHERE source = '%s'%s" % (", ".join(attributes) or "*", table, s, extra) for s in srcs)


if __name__ == "__main__":
    for v in SOURCE_VIEWS.values():
        print(v.definition()); print("   ho=%s, mui gio=UTC%+d, %s" % (v.os_family, v.tz_offset_hours, v.note))
    print("\nQ1: nguon nao ghi san ma tien trinh (pid)?      ->", select_sources(["host", "pid"], {"native": ["pid"]}))
    print("Q2: nguon nao ghi san muc log?                  ->", select_sources(["level"], {"native": ["level"]}))
    print("Q3: nguon nao co log trong thang 7/2017?        ->", select_sources(["content"], {"time_from": "2017-07-01", "time_to": "2017-08-01"}))
    print("Q4: nguon nao thuoc ho Unix?                    ->", select_sources([], {"os_family": "unix"}))
    print("\nViet lai Q3 thanh SQL:\n" + rewrite(["event_time", "host", "content"], {"time_from": "2017-07-01", "time_to": "2017-08-01"}))
